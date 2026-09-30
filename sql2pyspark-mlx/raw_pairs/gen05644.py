from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("event_time_day")
w_ordered = Window.partitionBy("page").orderBy("days_since_event_time", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("page").orderBy("event_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page") != "/home")
    .withColumn("event_time_day", F.col("event_time").cast("date"))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
)

ranked = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .withColumn("last7_avg_duration_sec", F.avg("duration_sec").over(w_ordered))
    .withColumn("last7_min_duration_sec", F.min("duration_sec").over(w_ordered))
    .withColumn("last7_max_duration_sec", F.max("duration_sec").over(w_ordered))
    .withColumn("running_days_since_event_time", F.sum("days_since_event_time").over(w_hist))
)

cleaned = (
    with_windows
    .filter(F.col("duration_sec") < 347)
    .withColumn("page_prefix", F.substring("page", 1, 4))
    .withColumn("last7_min_duration_sec_int", F.col("last7_min_duration_sec").cast("int"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("DIRECT")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("last7_max_duration_sec") >= 400, "HIGH").otherwise("LOW").alias("last7_max_duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.round(F.sum("last7_min_duration_sec") / F.nullif(F.count(F.when(F.col("event_type") == "search", 1)), F.lit(0)), 2).alias("last7_min_duration_sec_per_search"),
        F.sum(F.when(F.col("event_type") == "click", F.col("last7_min_duration_sec"))).alias("click_total_last7_min_duration_sec"),
    )
    .orderBy(F.col("last7_min_duration_sec_per_search").desc())
)
