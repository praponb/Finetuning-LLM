from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("session_start_flag", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy(F.month("started_at"), "browser").orderBy(F.col("started_at_day").desc(), "id")
w6 = Window.orderBy("started_at", "id")
w7 = Window.orderBy("started_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("country") == "UK") & (F.col("started_at") > F.to_date(F.lit("2023-05-13"))))
    .select(
        "*",
        F.dayofmonth("started_at").alias("started_at_day"),
        F.concat("device", F.lit(" - "), "country").alias("device_country"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_semi")
)

gaps = (
    eligible
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

with_windows = (
    sessionized
    .filter(F.col("session_start_flag") <= 1)
    .select(
        "*",
        F.max("session_start_flag").over(w3).alias("session_start_flag_max_7d"),
        F.avg("session_id").over(w4).alias("session_id_avg_3_prior"),
        F.ntile(4).over(w5).alias("started_at_day_bucket"),
        F.lead("started_at_day", 1).over(w6).alias("next_started_at_day"),
        F.sum("started_at_day").over(w7).alias("cumulative_started_at_day"),
    )
)

eligible_sessions = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

eligible_sessions_2 = (
    eligible_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

result = (
    eligible_sessions_2
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .rollup(F.col("c.segment").alias("segment"), F.col("a.browser").alias("browser"))
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.session_id_avg_3_prior")).alias("total_session_id_avg_3_prior"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("browser")
)
