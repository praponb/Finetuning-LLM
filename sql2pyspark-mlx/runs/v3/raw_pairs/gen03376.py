from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.col("started_at") >= "2025-04-01"))
    .select("id", "browser", "device", F.to_date("started_at").alias("started_at_date"))
    .orderBy("browser")
)
