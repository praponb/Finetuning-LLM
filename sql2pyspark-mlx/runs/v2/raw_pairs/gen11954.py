from pyspark.sql import functions as F

result = (
    df_sessions
    .filter((F.col("browser") == "Edge") & (F.col("customer_id") == 22) & F.col("country").isin(["TH", "US"]))
    .select(
        "id",
        "customer_id",
        "started_at",
        F.concat_ws("/", "device", "browser").alias("device_browser_key"),
    )
    .orderBy("device_browser_key", "id")
    .limit(20)
)
