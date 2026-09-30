from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "desktop", "tablet"]) & F.col("customer_id").isin([6, 30, 32, 35]))
    .select(
        "started_at",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
        F.substring("device", 1, 4).alias("device_prefix"),
    )
)
