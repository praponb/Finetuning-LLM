from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "started_at",
        F.when(F.col("device").isin(["mobile", "desktop"]), "Priority").otherwise("Other").alias("device_group"),
        F.substring("browser", 1, 4).alias("browser_prefix"),
    )
)
