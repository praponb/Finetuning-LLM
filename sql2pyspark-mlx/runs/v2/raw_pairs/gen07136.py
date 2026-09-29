from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") >= "2024-02-20")
    .select(
        "id",
        "device",
        "customer_id",
        F.lower("device").alias("device_lower"),
        F.lower("country").alias("country_lower"),
    )
)
