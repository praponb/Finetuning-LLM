from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("device") == "mobile") & (F.col("customer_id").isNull() | (F.col("country") == "UK")))
    .select(
        "started_at",
        "customer_id",
        "device",
        F.when(F.col("device") == "desktop", 1).otherwise(0).alias("is_desktop"),
    )
    .orderBy("started_at")
)
