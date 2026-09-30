from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-04-03")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
        F.concat_ws(",", "event_type", "page").alias("event_type_page_key"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_web_events
    .rollup(
        F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL").alias("new_group_flag_tier"),
    )
    .agg(
        F.avg(F.when(F.col("event_type") == "click", F.col("duration_sec"))).alias("click_avg_duration_sec"),
        F.percentile_approx("session_no", 0.9).alias("session_no_p90"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy(F.col("search_duration_sec").asc_nulls_last())
)
