from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-03-30 04:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        F.hour("event_time").alias("event_time_hour"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

prev_events = cleaned.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("session_id_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_id_2_tier"),
    )
    .agg(
        F.percentile_approx("session_id_2", [0.25, 0.5, 0.75]).alias("session_id_2_quartiles"),
        F.sum(F.when(F.col("event_type") == "click", F.col("new_group_flag"))).alias("click_total_new_group_flag"),
        F.percentile_approx("new_group_flag", 0.5).alias("new_group_flag_p50"),
    )
)
