from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "browser",
        "customer_id",
        F.when(F.col("device").isin(["desktop", "mobile"]), "Tier 1").otherwise("Other").alias("device_group"),
        F.trim("device").alias("clean_device"),
    )
    .orderBy("clean_device", F.col("browser").desc())
)
