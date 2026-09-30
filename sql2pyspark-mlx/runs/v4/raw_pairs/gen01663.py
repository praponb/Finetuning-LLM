from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isin([23, 31, 34, 35]) & (F.col("device") != "mobile") & (F.col("country") == "US"))
    .select("id", "country", "started_at", "browser")
    .orderBy(F.col("browser").desc(), F.col("id").desc())
)
