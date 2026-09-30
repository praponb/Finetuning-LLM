from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("started_at", "id")
w_running = Window.partitionBy("device").orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("web_event_duration_sec").desc(), "id")
w_part_2 = Window.partitionBy("device").orderBy("web_event_duration_sec", "id")
w7 = Window.partitionBy("page").orderBy(F.col("monthly_fee").asc_nulls_last())
w8 = Window.partitionBy("page").orderBy(F.col("cumulative_web_event_duration_sec").asc_nulls_first())
w9 = Window.orderBy("next_web_event_duration_sec", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("device").orderBy(F.col("cumulative_web_event_duration_sec").desc())
w11 = Window.partitionBy("plan").orderBy("started_at", "id", "subscription_id")
w12 = Window.partitionBy("plan").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_latest = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

enriched = (
    with_latest
    .withColumn("web_event_duration_sec_sum_7d", F.sum("web_event_duration_sec").over(w2))
    .withColumn("next_web_event_duration_sec", F.lead("web_event_duration_sec", 1).over(w_part))
    .withColumn("cumulative_web_event_duration_sec", F.sum("web_event_duration_sec").over(w_running))
    .withColumn("web_event_duration_sec_bucket", F.ntile(5).over(w5))
    .withColumn("row_seq", F.row_number().over(w_part_2))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.credit_limit"), F.col("b.city"), F.col("b.name"))
)

matched = (
    with_dims
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched_sessions = (
    matched
    .select(
        "*",
        F.cume_dist().over(w7).alias("monthly_fee_cume_dist"),
        F.rank().over(w8).alias("cumulative_web_event_duration_sec_rank"),
        F.sum("web_event_duration_sec").over(w9).alias("running_web_event_duration_sec"),
        F.rank().over(w10).alias("cumulative_web_event_duration_sec_rank_2"),
    )
)

prev_events = enriched_sessions.withColumn("prev_started_at", F.lag("started_at").over(w11))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w12))

result = (
    sessionized
    .withColumn("running_web_event_duration_sec_tier", F.when(F.col("running_web_event_duration_sec") >= 1806, "HIGH").otherwise("LOW"))
    .rollup("page", "running_web_event_duration_sec_tier")
    .agg(
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("prev_started_at")) - F.unix_timestamp(F.min("prev_started_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_rows"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        ((F.unix_timestamp(F.max("prev_started_at")) - F.unix_timestamp(F.min("prev_started_at"))) / 60).alias("duration_min"),
        F.min("prev_started_at").alias("first_seen"),
        F.grouping("page").alias("grouping_level"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
