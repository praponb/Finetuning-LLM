from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_type") != "add_to_cart") & F.col("page").isNotNull())
    .select(
        "*",
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
        F.date_format("event_time", "yyyy-MM").alias("event_time_label"),
        F.coalesce("page", F.lit("NONE")).alias("page_filled"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .cube("island_id", "event_type", "week_start")
    .agg(
        F.count_if(F.col("page") == "/home").alias("home_events"),
        F.sum(F.when(F.col("event_type") == "click", F.col("session_start_flag")).otherwise(0)).alias("click_session_start_flag"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("duration_sec_per_search"),
        F.avg(F.when(F.col("event_type") == "page_view", F.col("session_start_flag"))).alias("page_view_avg_session_start_flag"),
        F.grouping("island_id").alias("gid"),
    )
)
