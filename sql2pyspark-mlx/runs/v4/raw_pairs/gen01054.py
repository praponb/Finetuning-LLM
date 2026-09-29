from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy("started_at", "id", "subscription_id")
w3 = Window.partitionBy("device").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("device").orderBy(F.col("session_no").desc(), "id", "subscription_id")
w6 = Window.partitionBy("customer_id").orderBy("duration_sec", "id", "subscription_id")

with_window = (
    df_sessions
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_last = (
    with_window
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

with_gaps = (
    with_last
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w3))

enriched = (
    islands
    .withColumn("monthly_fee_avg_30d", F.avg("monthly_fee").over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("seq_no_2", F.row_number().over(w6))
)

filtered = (
    enriched
    .filter(F.col("started_at") >= F.lit("2023-03-25 03:00:00").cast("timestamp"))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "subscription_id",
        "plan",
        "monthly_fee",
        "duration_sec",
        "session_start_flag",
        "monthly_fee_avg_30d",
        "seq_no",
        "seq_no_2",
        F.upper("country").alias("country_upper"),
        (F.col("monthly_fee_avg_30d") - F.col("seq_no")).alias("monthly_fee_avg_30d_minus_seq_no"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

valid = (
    latest_match
    .filter(F.col("started_at") > F.to_timestamp(F.lit("2023-04-16 16:00:00")))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "subscription_id",
        "plan",
        "monthly_fee",
        "duration_sec",
        "session_start_flag",
        "monthly_fee_avg_30d",
        "seq_no",
        "seq_no_2",
        "country_upper",
        "monthly_fee_avg_30d_minus_seq_no",
        "web_event_event_time",
        F.when(F.col("web_event_event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
        F.coalesce("plan", F.lit("DIRECT")).alias("resolved_plan"),
    )
)

result = (
    valid
    .groupBy(
        "plan",
        "device",
        F.when(F.col("seq_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("seq_no_tier"),
    )
    .agg(
        F.round(F.sum("is_purchase") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("is_purchase_per_uk"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        ((F.unix_timestamp(F.max("web_event_event_time")) - F.unix_timestamp(F.min("web_event_event_time"))) / 60).alias("span_minutes"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").asc_nulls_first())
)
