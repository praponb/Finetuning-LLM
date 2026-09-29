from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "desktop"]) & (F.col("country") != "US") & F.col("country").isin(["US", "JP", "SG"]))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.substring("device", 1, 4).alias("device_prefix"),
    )
)
