from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-10-03 20:00:00").cast("timestamp")) & (F.col("session_id") == 1))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.floor("duration_sec").alias("duration_sec_whole"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("session_no")).otherwise(0)).alias("search_session_no"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.percentile_approx("duration_sec_whole", [0.25, 0.5, 0.75]).alias("duration_sec_whole_quartiles"),
        F.avg(F.when(F.col("event_type") == "search", F.col("duration_sec_whole"))).alias("search_avg_duration_sec_whole"),
    )
    .orderBy(F.col("search_session_no").desc_nulls_first())
)
