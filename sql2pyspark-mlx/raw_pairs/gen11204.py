from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/home"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
        F.lower("event_type").alias("event_type_lower"),
    )
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "event_type_lower",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.percentile_approx("burst_id", 0.5).alias("burst_id_p50"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
