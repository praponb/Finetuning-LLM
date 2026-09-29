from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull() & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.concat_ws(",", "browser", "country").alias("browser_country_key"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_filled"),
    )
)

kept_keys = (
    filtered
    .select("country", "customer_id")
    .subtract(
        df_sessions
        .filter(~F.col("browser").isin(["Safari", "Firefox", "Chrome"]))
        .select("country", "customer_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.duration_sec") <= 187), "left_anti")
)

filtered_sessions = (
    kept
    .filter(F.col("device").isin(["mobile", "tablet", "desktop"]))
    .withColumn("browser_length", F.length("browser"))
    .withColumn("country_filled", F.coalesce("country", F.lit("N/A")))
)

result = (
    filtered_sessions
    .groupBy(
        F.when(F.col("browser_length") >= 74, "HIGH").otherwise("LOW").alias("browser_length_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
        F.avg(F.when(F.col("country") == "JP", F.col("browser_length"))).alias("jp_avg_browser_length"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
    .filter(F.col("th_count") > 1)
    .orderBy(F.col("browser_length_tier").desc_nulls_first())
)
