from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .withColumn("week_start", F.date_trunc("week", "event_time"))
    .groupingSets(
        [["week_start", "event_type", "page"], ["week_start", "event_type"], ["week_start"], []],
        "week_start",
        "event_type",
        "page",
    )
    .agg(
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_rows"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
    )
)
