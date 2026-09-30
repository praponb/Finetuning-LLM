from pyspark.sql import functions as F

valid = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-12-02 10:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .withColumn("browser_prefix", F.substring("browser", 1, 2))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("N/A")))
)

remaining_keys = (
    valid
    .select("customer_id")
    .intersectAll(
        df_sessions
        .filter(F.col("customer_id").isin([22, 23, 34, 39]))
        .select("customer_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.country").alias("customer_country"))
)

kept = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

matched_keys = (
    kept
    .select("id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isNull())
        .select("id")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.name"))
)

result = (
    enriched
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .rollup("year_start")
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.size(F.collect_set("country")).alias("num_countries_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("num_countries") >= 1)
)
