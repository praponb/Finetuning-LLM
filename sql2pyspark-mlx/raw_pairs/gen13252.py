from pyspark.sql import functions as F

joined = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.browser") == "Edge")
    .select("s.id", "s.session_id", F.col("d.browser"), F.col("d.device"))
)

kept_keys = (
    joined
    .select("id")
    .exceptAll(
        df_web_events
        .filter(F.col("duration_sec") > 418)
        .select("id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("browser").isNotNull())
    .withColumn("device_browser_key", F.concat_ws(",", "device", "browser"))
    .withColumn("device_filled", F.coalesce("device", F.lit("OTHER")))
)

valid = (
    filtered
    .filter(F.col("device").isNotNull() & F.col("session_id").isin([12, 40]))
    .withColumn("is_tablet", F.when(F.col("device") == "tablet", 1).otherwise(0))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("N/A")))
)

result = (
    valid
    .groupBy("device")
    .pivot("browser", ["Chrome", "Firefox", "Edge"])
    .agg(F.sum("is_tablet").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
