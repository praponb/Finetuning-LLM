from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_web_events
    .groupBy(F.date_trunc("MONTH", "prev_event_time").alias("activity_month"))
    .agg(
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.percentile_approx("new_group_flag", 0.5).alias("new_group_flag_p50"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "search", 1)), F.lit(0)), 2).alias("duration_sec_per_search"),
        F.avg(F.when(F.col("event_type") == "add_to_cart", F.col("new_group_flag"))).alias("add_to_cart_avg_new_group_flag"),
        F.sum("session_no").alias("total_session_no"),
    )
    .filter(F.col("add_to_cart_avg_new_group_flag") >= 0)
)
