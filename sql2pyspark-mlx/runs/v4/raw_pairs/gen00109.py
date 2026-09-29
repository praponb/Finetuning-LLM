from pyspark.sql import functions as F

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-08-07 06:00:00").cast("timestamp"))
    .withColumn("started_at_period", F.date_trunc("week", "started_at"))
)

remaining_keys = (
    base
    .select("browser", "id")
    .subtract(
        df_sessions
        .filter(~F.col("country").isin(["UK", "SG"]))
        .select("browser", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("id")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.email"), F.col("c.name"))
)

kept_keys = (
    enriched
    .select("country")
    .exceptAll(
        df_sessions
        .filter(F.col("device") != "mobile")
        .select("country")
    )
)

kept_rows_sessions = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

enriched_sessions = (
    kept_rows_sessions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.name").alias("customer_name"))
)

result = (
    enriched_sessions
    .groupBy("browser")
    .agg(
        F.max("started_at_period").alias("ended_at"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        ((F.unix_timestamp(F.max("started_at_period")) - F.unix_timestamp(F.min("started_at_period"))) / 60).alias("span_minutes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("country") == "US").alias("us_events"),
    )
    .orderBy("distinct_ids")
)
