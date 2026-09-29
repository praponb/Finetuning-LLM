from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("session_id", "event_type").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("running_avg_duration_sec", "id")
w_ordered = Window.partitionBy("country").orderBy("next_running_avg_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.partitionBy("event_type", "country").orderBy("prev_running_avg_duration_sec", "id")

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

joined = (
    sessions_web_events
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.browser"), F.col("b.country"))
)

enriched = (
    joined
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "country",
        F.avg("duration_sec").over(w_part).alias("running_avg_duration_sec"),
        F.min("duration_sec").over(w_part).alias("running_min_duration_sec"),
        F.sum("duration_sec").over(w_part).alias("running_sum_duration_sec"),
    )
)

metrics = (
    enriched
    .withColumn("prev_running_min_duration_sec", F.lag("running_min_duration_sec").over(w4))
    .withColumn("next_running_avg_duration_sec", F.lead("running_avg_duration_sec").over(w4))
    .withColumn("prev_running_avg_duration_sec", F.lag("running_avg_duration_sec").over(w4))
    .withColumn("seq_no", F.row_number().over(w4))
)

filtered = (
    metrics
    .filter(F.col("next_running_avg_duration_sec") <= 330)
    .select(
        "*",
        F.when(F.col("page") == "/cart", 1).otherwise(0).alias("is_cart"),
        (F.col("next_running_avg_duration_sec") / F.nullif("running_sum_duration_sec", F.lit(0))).alias("next_running_avg_duration_sec_per_running_sum_duration_sec"),
    )
)

metrics_web_events = (
    filtered
    .select(
        "id",
        "event_type",
        "running_min_duration_sec",
        "prev_running_min_duration_sec",
        "next_running_avg_duration_sec",
        "prev_running_avg_duration_sec",
        "seq_no",
        "is_cart",
        F.min("prev_running_avg_duration_sec").over(w_ordered).alias("running_min_prev_running_avg_duration_sec"),
        F.count("prev_running_avg_duration_sec").over(w_ordered).alias("running_count_prev_running_avg_duration_sec"),
        F.max("prev_running_avg_duration_sec").over(w_ordered).alias("running_max_prev_running_avg_duration_sec"),
        F.lag("running_min_duration_sec", 1).over(w_ordered_2).alias("prev_running_min_duration_sec_2"),
    )
)

result = (
    metrics_web_events
    .groupBy(
        F.when(F.col("next_running_avg_duration_sec") >= 440, "PREMIUM").otherwise("STANDARD").alias("next_running_avg_duration_sec_tier"),
        "event_type",
        F.when(F.col("is_cart") >= 68, "PREMIUM").otherwise("STANDARD").alias("is_cart_tier"),
    )
    .agg(
        F.count_if(F.col("event_type") == "page_view").alias("page_view_events"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
    .orderBy("event_type")
)
