from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "started_at",
        F.to_date("started_at").alias("started_at_date"),
        F.lower("device").alias("device_lower"),
    )
)
