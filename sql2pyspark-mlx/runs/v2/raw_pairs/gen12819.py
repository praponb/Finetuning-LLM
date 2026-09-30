from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "mobile", "tablet"]))
    .select(
        "device",
        F.when(F.col("device").isin(["mobile", "tablet"]), "Group A").otherwise("Other").alias("device_group"),
    )
)
