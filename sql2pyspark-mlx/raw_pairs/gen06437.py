from pyspark.sql import functions as F

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-04-14 08:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country").isin(["SG", "JP"]), "Group A").otherwise("Other").alias("country_group"),
        F.trim("browser").alias("clean_browser"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_clean"),
    )
)

matched_keys = (
    cleaned
    .select("customer_id", "device")
    .exceptAll(
        df_sessions
        .filter(F.col("browser").isNotNull())
        .select("customer_id", "device")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("started_at") > F.lit("2023-06-24").cast("date"))
    .withColumn("started_at_period", F.date_trunc("YEAR", "started_at"))
    .withColumn("browser_clean_2", F.coalesce("browser", F.lit("NONE")))
)

not_flagged = (
    base
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type") == "page_view"), "left_anti")
)

summary = (
    not_flagged
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.max("started_at").alias("last_started_at"),
        ((F.unix_timestamp(F.max("started_at_period")) - F.unix_timestamp(F.min("started_at_period"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
)

result = (
    summary
    .select(
        "country",
        "device_list",
        "last_started_at",
        "span_minutes",
        "num_browsers",
        F.when(F.col("country").rlike("^(JP|TH)$"), "GROUP_A").when(F.col("country").rlike("^(US|SG)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("country_zone"),
    )
)
