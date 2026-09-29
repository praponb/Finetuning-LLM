from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-30 10:00:00").cast("timestamp")) & F.col("session_id").isin([3, 7, 31, 34]))
    .withColumn("months_since_event_time", (F.datediff(F.current_date(), "event_time") / 30))
    .withColumn("clean_event_type", F.trim("event_type"))
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "months_since_event_time",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .cube(
        "burst_id",
        F.when(F.col("duration_sec") >= 374, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.round(F.sum("months_since_event_time") / F.nullif(F.count(F.when(F.col("event_type") == "click", 1)), F.lit(0)), 2).alias("months_since_event_time_per_click"),
    )
    .orderBy(F.col("duration_sec_tier").desc_nulls_last())
)
