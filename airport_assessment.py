import io, pandas as pd

data = 'Airline Code;DelayTimes;FlightCodes;To_From\nAir Canada (!);[21, 40];20015.0;WAterLoo_NEWYork\n<Air France> (12);[];;Montreal_TORONTO\n(Porter Airways. );[60, 22, 87];20035.0;CALgary_Ottawa\n12. Air France;[78, 66];;Ottawa_VANcouvER\n""".\\.Lufthansa.\\.""";[12, 33];20055.0;london_MONTreal\n'
#parsing raw data into a csv
def read_raw_data(raw: str) -> pd.DataFrame:
    df=pd.read_csv(io.StringIO(raw), sep=";")
    expected = ["Airline Code", "DelayTimes", "FlightCodes", "To_From"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")
    return df

#cleaning airline code values 
def clean_airlines(airlines: pd.Series) -> pd.Series:
    cleaned = (airlines.astype("string")
               .str.replace(r'[^A-Za-z ]+', ' ', regex=True)
               .str.replace(r'\s+', ' ', regex=True)
               .str.strip())
    return cleaned

#splitting up the to_from column and cleaning each column for readability
def split_to_from (series: pd.Series) -> pd.DataFrame:
    upper = series.astype('string').str.upper()

    parts= upper.str.extract(r"^\s*([^_\-/|]+)[_\-/|]\s*(.+?)\s*$")
    parts.columns = ['To', 'From']
    return parts.apply(lambda col: col.str.replace(r'[^A-Z]', '', regex=True))

#filling null flightcode rows and ensuring flightcode stepping up by 10
def fill_flightcodes(series: pd.Series, step: int = 10, default_start: int = 1010)-> pd.Series:
    codes = pd.to_numeric(series, errors="coerce")
    first_idx = codes.first_valid_index()
    if first_idx is not None:
        start = int(codes.loc[first_idx]) - first_idx * step
    else:
        start = default_start
    return (pd.RangeIndex(len(series)) * step+start).astype('int64')

#combining the above helper functions to create the final dataframe
def transform(raw: str) ->pd.DataFrame:

    df = read_raw_data(raw)
    df['Airline Code'] = clean_airlines(df['Airline Code'])
    df['FlightCodes'] = fill_flightcodes(df['FlightCodes'])
    tf = split_to_from(df['To_From'])
    #dropping the old to_from column from the new table and joining the split up to and from columns into the new transformed dataframe 
    df = df.drop(columns=['To_From']).join(tf)

    return df[['Airline Code', 'DelayTimes', 'FlightCodes', 'To', 'From']].convert_dtypes()


if __name__ == "__main__":
    RAW = 'Airline Code;DelayTimes;FlightCodes;To_From\nAir Canada (!);[21, 40];20015.0;WAterLoo_NEWYork\n<Air France> (12);[];;Montreal_TORONTO\n(Porter Airways. );[60, 22, 87];20035.0;CALgary_Ottawa\n12. Air France;[78, 66];;Ottawa_VANcouvER\n""".\\.Lufthansa.\\.""";[12, 33];20055.0;london_MONTreal\n'
    transformed_data=transform(RAW)
    print (transformed_data)