from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("device")
    .intersect(
        df_sessions
        .filter(F.col("customer_id").isin([3, 6, 7]))
        .select("device")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.when(F.col("device").rlike("^(mobile|desktop)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("device_zone"),
    )
)
