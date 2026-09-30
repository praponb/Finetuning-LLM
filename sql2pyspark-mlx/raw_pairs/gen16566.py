from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.date_trunc("year", "event_time").alias("year_start"),
        F.date_trunc("month", "event_time").alias("activity_month"),
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.max(F.when(F.col("page") == "/search", F.col("island_id"))).alias("search_max_island_id"),
        F.count_if(F.col("page") == "/search").alias("search_events"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.sort_array(F.collect_set("page")).alias("page_list_2"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .orderBy("span_minutes")
)
