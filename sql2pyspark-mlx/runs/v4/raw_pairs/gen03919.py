from pyspark.sql import functions as F

kept_keys = (
    df_sessions
    .select("id")
    .intersectAll(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

verified = (
    df_sessions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "CORE").otherwise("GROUP_B").alias("device_zone"),
    )
    .orderBy("customer_id")
)
