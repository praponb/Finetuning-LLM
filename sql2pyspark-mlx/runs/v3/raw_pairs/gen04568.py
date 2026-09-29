from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-07-06")
    .select(
        "id",
        "started_at",
        F.trim("device").alias("clean_device"),
        F.col("started_at").cast("date").alias("started_at_day"),
    )
    .orderBy("started_at_day", F.col("started_at").desc())
)
