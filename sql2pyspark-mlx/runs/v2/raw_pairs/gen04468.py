from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(((F.col("started_at") >= "2025-01-01") & F.col("customer_id").isNull()) | F.col("customer_id").isNotNull())
    .select(
        "id",
        "device",
        "started_at",
        "customer_id",
        F.length("browser").alias("browser_length"),
    )
)
