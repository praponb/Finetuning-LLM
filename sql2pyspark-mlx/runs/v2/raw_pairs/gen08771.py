from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser") == "Firefox")
    .select("id", "country")
    .unionByName(df_sessions.filter(F.col("country").isNull()).select("id", "country"))
)
