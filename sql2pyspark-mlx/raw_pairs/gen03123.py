from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull() & F.col("country").isin(["US", "UK"]))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.when(F.col("browser").isin(["Chrome", "Safari"]), "Core").otherwise("Other").alias("browser_group"),
    )
)

result = (
    cleaned
    .groupBy("device")
    .agg(
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.min("started_at").alias("started_at"),
    )
)
