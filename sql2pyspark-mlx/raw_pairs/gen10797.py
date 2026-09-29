from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("page") != "/cart") & F.col("page").isNotNull())
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM-dd"))
    .withColumn("event_type_length", F.length("event_type"))
    .withColumn("page_clean", F.coalesce("page", F.lit("NONE")))
)

prev_events = valid.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "HIGH").otherwise("LOW"))
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 7, "LARGE").otherwise("SMALL"))
    .rollup("is_new_session_tier", "session_id", "session_no_tier")
    .agg(
        F.max(F.when(F.col("event_type") == "click", F.col("is_new_session"))).alias("click_max_is_new_session"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.sum(F.when(F.col("page") == "/search", F.col("is_new_session")).otherwise(0)).alias("search_is_new_session"),
        F.sum(F.when(F.col("event_type") == "search", F.col("session_no")).otherwise(0)).alias("search_session_no"),
        ((F.unix_timestamp(F.max("prev_event_time")) - F.unix_timestamp(F.min("prev_event_time"))) / 60).alias("duration_min"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("click_max_is_new_session") > 0)
)
