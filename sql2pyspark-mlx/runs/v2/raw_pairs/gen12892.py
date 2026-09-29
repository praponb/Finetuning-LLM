from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id") == 6)
    .select(
        "device",
        "browser",
        F.lower("browser").alias("browser_lower"),
        F.when(F.col("device").isin(["desktop", "tablet"]), "Tier 1").otherwise("Other").alias("device_group"),
    )
    .orderBy("device_group")
)
