from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy("age", "id").rowsBetween(-5, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-04-04 20:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        F.col("c.event_time").alias("web_event_event_time"),
    )
)

enriched = (
    latest_match
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.web_event_event_time",
        F.col("c.age"),
        F.col("c.name"),
        F.col("c.credit_limit"),
    )
)

enriched_sessions = (
    enriched
    .withColumn("customer_id_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w_grp))
    .withColumn("age_avg_5", F.avg("age").over(w3))
)

matched_keys = (
    enriched_sessions
    .select("customer_id", "id")
    .subtract(
        df_sessions
        .filter(F.to_date("started_at") >= "2023-09-01")
        .select("customer_id", "id")
    )
)

reconciled = (
    enriched_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([15, 16, 36, 37])), "left_anti")
)

result = (
    eligible
    .withColumn("age_avg_5_tier", F.when(F.col("age_avg_5") >= 69, "PREMIUM").otherwise("STANDARD"))
    .withColumn("customer_id_stddev_samp_credit_limit_tier", F.when(F.col("customer_id_stddev_samp_credit_limit") >= 15368, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "web_event_event_time"))
    .cube("age_avg_5_tier", "customer_id_stddev_samp_credit_limit_tier", "year_start")
    .agg(
        F.max("age_avg_5").alias("max_age_avg_5"),
        F.min("age").alias("min_age"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.min("customer_id_stddev_samp_credit_limit").alias("min_customer_id_stddev_samp_credit_limit"),
        F.count("*").alias("event_count"),
        F.max("web_event_event_time").alias("last_web_event_event_time"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("age_p95") >= 0)
    .orderBy(F.col("customer_id_stddev_samp_credit_limit_tier").desc_nulls_last())
)
