from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-13 20:00:00").cast("timestamp")) & (F.col("event_type") == "add_to_cart"))
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.col("duration_sec").cast("double").alias("duration_sec_str"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .withColumn("duration_sec_str_tier", F.when(F.col("duration_sec_str") >= 70, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .groupingSets(
        [["session_id", "duration_sec_str_tier", "activity_month"], ["session_id", "duration_sec_str_tier"]],
        "session_id",
        "duration_sec_str_tier",
        "activity_month",
    )
    .agg(
        F.min("session_no").alias("min_session_no"),
        F.percentile_approx("duration_sec_str", [0.25, 0.5, 0.75]).alias("duration_sec_str_quartiles"),
        F.min("session_no").alias("min_session_no_2"),
    )
    .orderBy("duration_sec_str_tier")
)
