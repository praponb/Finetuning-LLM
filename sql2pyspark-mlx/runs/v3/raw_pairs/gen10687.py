from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("started_at") < "2025-08-16")
    .select(
        "customer_id",
        "browser",
        "device",
        F.lower("country").alias("country_lower"),
    )
)
