from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("event_time_date", F.to_date("event_time"))
    .withColumn("page_event_type", F.concat("page", F.lit(" - "), "event_type"))
    .withColumn("page_clean", F.coalesce("page", F.lit("DIRECT")))
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .groupingSets(
        [["year_start", "activity_month"], ["year_start"], []],
        "year_start",
        "activity_month",
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.countDistinct("page").alias("distinct_pages"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.grouping("year_start").alias("grouping_level"),
    )
    .orderBy(F.col("avg_island_id").desc())
)
