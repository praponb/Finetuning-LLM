from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Safari") & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "started_at",
        "device",
        F.upper("browser").alias("browser_upper"),
        F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"),
    )
)
