from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w_hist = Window.partitionBy("page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("device").orderBy("duration_sec", "id")
w4 = Window.partitionBy("browser").orderBy("event_time", "id")
w5 = Window.partitionBy("browser").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

enriched = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.browser"), F.col("s.device"))
)

scored = (
    enriched
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lag("duration_sec", 1).over(w_hist).alias("prev_duration_sec"),
        F.lag("duration_sec", 1).over(w3).alias("prev_duration_sec_2"),
    )
)

with_gaps = (
    scored
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w5))

pivoted = (
    islands
    .groupBy("session_id", "browser")
    .pivot("device", ["tablet", "mobile"])
    .agg(F.avg("row_seq").alias("total"), F.count(F.lit(1)).alias("item_count"))
)

result = (
    pivoted
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "browser",
        "tablet_total",
        "mobile_total",
        "mobile_item_count",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("session_id").asc_nulls_first())
)
