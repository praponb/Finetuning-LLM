from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("customer_id") == 9) | F.col("country").isin(["UK", "US", "DE"]))
    .select(
        "id",
        "device",
        "started_at",
        "customer_id",
        F.trim("device").alias("clean_device"),
    )
    .orderBy("clean_device")
)
