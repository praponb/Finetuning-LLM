from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2024-04-01")
    .select(
        "id",
        "started_at",
        "customer_id",
        "device",
        F.dayofmonth("started_at").alias("started_at_day"),
    )
)
