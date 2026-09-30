from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") < "2023-03-01")
    .select("region")
    .subtract(df_sessions.select("browser"))
)
