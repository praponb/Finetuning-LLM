from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Edge"]) & F.col("country").isNull())
    .select(
        "id",
        "started_at",
        F.concat_ws(",", "device", "browser").alias("device_browser_key"),
    )
)
