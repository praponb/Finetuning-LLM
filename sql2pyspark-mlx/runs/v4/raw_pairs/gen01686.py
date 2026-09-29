from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-12-30 17:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("browser").isin(["Edge", "Chrome"]), "Core").otherwise("Other").alias("browser_group"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"))
)

kept_keys = (
    enriched
    .select("customer_id", "device")
    .intersect(
        df_sessions
        .filter(F.col("country") == "SG")
        .select("customer_id", "device")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("started_at") > F.lit("2024-01-10 14:00:00").cast("timestamp")) & (F.col("started_at") >= "2023-09-01"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "browser_group",
        "country_filled",
        "city",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

matched_keys = (
    filtered
    .select("customer_id")
    .intersectAll(
        df_sessions
        .filter(F.to_date("started_at") >= "2023-09-03")
        .select("customer_id")
    )
)

verified_sessions = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified_sessions
    .filter(F.col("started_at") > F.lit("2023-05-25").cast("date"))
    .select(
        "id",
        "device",
        "browser",
        "country",
        "browser_group",
        "country_filled",
        "country_title",
        "city_clean",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

base_sessions = (
    cleaned
    .filter(F.col("device") == "mobile")
    .select(
        "device",
        "browser",
        "country",
        "browser_group",
        "country_filled",
        "country_title",
        "started_at_label",
        "city_title",
        "city_filled",
        F.lower("device").alias("device_lower"),
    )
)

daily = (
    base_sessions
    .groupBy("browser")
    .agg(
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
    )
)

result = (
    daily
    .select("browser", "firefox_events")
    .orderBy("browser")
    .limit(10)
)
