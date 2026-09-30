from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "session_id",
        "event_time",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.when(F.col("island_id") >= 9, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.min("event_time").alias("first_seen"),
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
    )
    .orderBy(F.col("avg_island_id").desc())
)
