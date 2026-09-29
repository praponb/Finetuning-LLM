from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page")
w3 = Window.partitionBy("event_type").orderBy("page_min_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("page").orderBy("cumulative_duration_sec", "id")
w6 = Window.partitionBy("session_id").orderBy("event_time", "id")
w7 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

scored = (
    df_web_events
    .select(
        "*",
        F.first("duration_sec", ignorenulls=True).over(w).alias("first_known_duration_sec"),
        F.sum("duration_sec").over(w).alias("cumulative_duration_sec"),
        F.min("duration_sec").over(w2).alias("page_min_duration_sec"),
    )
)

enriched = (
    scored
    .withColumn("running_page_min_duration_sec", F.sum("page_min_duration_sec").over(w3))
    .withColumn("cumulative_duration_sec_avg_30d", F.avg("cumulative_duration_sec").over(w4))
    .withColumn("next_first_known_duration_sec", F.lead("first_known_duration_sec", 1).over(w5))
)

flagged = (
    enriched
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w6).isNull() | (F.col("event_time") > F.lag("event_time").over(w6) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("new_group_flag").over(w7))

result = (
    islands
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "first_known_duration_sec",
        "page_min_duration_sec",
        "cumulative_duration_sec_avg_30d",
        "next_first_known_duration_sec",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("page").desc_nulls_last())
)
