from pyspark.sql import functions as F

filtered = (
    df_sessions
    .filter(F.col("browser").isNotNull() & F.col("country").isin(["SG", "JP", "US"]))
    .withColumn("country_device", F.concat("country", F.lit(", "), "device"))
    .withColumn("country_prefix", F.substring("country", 1, 4))
)

remaining_keys = (
    filtered
    .select("customer_id", "country")
    .intersect(
        df_sessions
        .filter(F.col("device") != "mobile")
        .select("customer_id", "country")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("started_at") >= F.lit("2023-07-28").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "country_device",
        "country_prefix",
        F.concat("browser", F.lit(", "), "device").alias("browser_device"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_clean"),
    )
)

filtered_sessions = (
    cleaned
    .filter(F.col("country").isin(["TH", "DE"]))
    .select(
        "*",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.length("country").alias("country_length"),
    )
)

joined = (
    filtered_sessions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.segment") == "Consumer")
    .select("a.*", F.col("d.segment"), F.col("d.country").alias("customer_country"))
)

matched_keys = (
    joined
    .select("customer_id", "country")
    .intersect(
        df_sessions
        .filter(F.col("device") == "mobile")
        .select("customer_id", "country")
    )
)

verified_sessions = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified_sessions
    .select(
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "country_device",
        "country_prefix",
        "browser_device",
        "started_at_day",
    )
)
