from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["mobile", "desktop", "tablet"]) & (F.col("browser") == "Edge") & (F.to_date("started_at") < "2023-06-11"))
    .select(
        "id",
        "device",
        F.trim("country").alias("clean_country"),
        F.lower("device").alias("device_lower"),
    )
)
