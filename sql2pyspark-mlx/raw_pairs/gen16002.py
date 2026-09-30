from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_country").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("city").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_country").orderBy("age", "id")
w5 = Window.partitionBy("browser").orderBy("running_sum_age", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("customer_id", "device").orderBy(F.col("session_start_flag").desc_nulls_last())

enriched = (
    df_sessions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.segment").isin(["Corporate", "Consumer"]))
    .select(
        "src.*",
        F.col("b.country").alias("customer_country"),
        F.col("b.city"),
        F.col("b.age"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

prev_events = not_flagged.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

scored = (
    sessions_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "session_start_flag",
        F.max("age").over(w_hist).alias("running_max_age"),
        F.sum("age").over(w_hist).alias("running_sum_age"),
        F.min("age").over(w_hist).alias("running_min_age"),
        F.ntile(5).over(w4).alias("age_bucket"),
    )
)

enriched_sessions = (
    scored
    .select(
        "*",
        F.avg("running_min_age").over(w5).alias("last4_avg_running_min_age"),
        F.min("running_min_age").over(w5).alias("last4_min_running_min_age"),
        F.sum("running_min_age").over(w5).alias("last4_sum_running_min_age"),
        F.cume_dist().over(w6).alias("session_start_flag_cume_dist"),
    )
)

grouped = (
    enriched_sessions
    .groupBy("browser", F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.sum("last4_avg_running_min_age").alias("total_last4_avg_running_min_age"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.max("last4_min_running_min_age").alias("max_last4_min_running_min_age"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
)

result = (
    grouped
    .select(
        "browser",
        "year_start",
        "total_last4_avg_running_min_age",
        "max_last4_min_running_min_age",
        "span_minutes",
    )
    .orderBy(F.col("total_last4_avg_running_min_age").asc_nulls_first(), "span_minutes")
)
