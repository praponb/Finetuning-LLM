from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy("event_time", "id")
w3 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("event_time", "id")
w5 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy(F.col("duration_sec").desc_nulls_last())
w7 = Window.partitionBy("country").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_running = Window.partitionBy("device")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-07-18").cast("date")) & (~F.col("browser").eqNullSafe(F.lit("Firefox"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.substring("device", 1, 2).alias("device_prefix"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("s.*", F.col("c.email"), F.col("c.country").alias("customer_country"))
)

latest_match = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.duration_sec"))
)

with_prev = latest_match.withColumn("prev_event_time", F.lag("event_time").over(w2))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

with_gaps_2 = (
    sessions_sessions
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions_2 = with_gaps_2.withColumn("session_no_2", F.sum("new_group_flag").over(w5))

eligible = (
    sessions_sessions_2
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.prev_event_time")), "left_anti")
)

with_windows = (
    eligible
    .select(
        "*",
        F.dense_rank().over(w6).alias("duration_sec_dense_rank"),
        F.count("duration_sec").over(w7).alias("duration_sec_count_7d"),
        F.stddev_samp("session_no").over(w_running).alias("device_stddev_samp_session_no"),
    )
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "device_prefix",
        "customer_country",
        "event_time",
        "duration_sec",
        "session_no_2",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "GROUP_B").otherwise("CORE").alias("device_zone"),
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("country").asc_nulls_last())
)
