from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("country").isin(["US", "DE"]))
    .select("id", "browser", "country", F.upper("country").alias("country_upper"))
)
