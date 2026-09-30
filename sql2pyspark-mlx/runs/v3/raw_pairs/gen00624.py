from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.to_date("started_at") < "2025-03-07"))
    .select("device", F.trim("device").alias("clean_device"))
    .orderBy(F.col("device").desc())
)
