from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("duration_sec").asc_nulls_last())
w3 = Window.partitionBy("session_id").orderBy("event_time", "id")
w4 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_hist = Window.partitionBy("event_type").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy("running_max_next_duration_sec", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("session_id").orderBy("duration_sec_max_7d", "id").rowsBetween(-5, -1)

with_windows = (
    df_web_events
    .withColumn("cumulative_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w2))
    .withColumn("next_duration_sec", F.lead("duration_sec", 2).over(w3))
    .withColumn("duration_sec_max_7d", F.max("duration_sec").over(w4))
)

with_gaps = (
    with_windows
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w))

metrics = (
    sessionized
    .select(
        "*",
        F.sum("next_duration_sec").over(w_hist).alias("running_sum_next_duration_sec"),
        F.avg("next_duration_sec").over(w_hist).alias("running_avg_next_duration_sec"),
        F.max("next_duration_sec").over(w_hist).alias("running_max_next_duration_sec"),
    )
)

metrics_web_events = (
    metrics
    .filter(F.col("running_sum_next_duration_sec") <= 2000)
    .withColumn("running_avg_next_duration_sec_avg_5", F.avg("running_avg_next_duration_sec").over(w6))
    .withColumn("duration_sec_percent_rank_avg_5_prior", F.avg("duration_sec_percent_rank").over(w7))
)

cleaned = (
    metrics_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-10-18"))) & F.col("page").isNotNull())
    .withColumn("duration_sec_max_7d_filled", F.coalesce("duration_sec_max_7d", F.lit(0)))
    .withColumn("event_time_period", F.date_trunc("WEEK", "event_time"))
)

wide = (
    cleaned
    .groupBy("session_id")
    .pivot("event_type", ["search", "page_view", "add_to_cart"])
    .agg(F.avg("running_sum_next_duration_sec"))
)

result = wide.select("session_id", "page_view")
