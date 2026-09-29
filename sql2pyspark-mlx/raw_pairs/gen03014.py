from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("session_id_2", "id")
w4 = Window.partitionBy("event_type").orderBy(F.col("session_start_flag").desc(), "id")
w5 = Window.partitionBy("session_id").orderBy("burst_id", "id")
w6 = Window.partitionBy("event_type").orderBy("session_id_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("event_type")

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-06-08"))) & F.col("page").isNotNull())
    .withColumn("clean_event_type", F.trim("event_type"))
    .withColumn("page_filled", F.coalesce("page", F.lit("N/A")))
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "page_filled",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

gaps = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w2))

scored = (
    sessions_web_events
    .withColumn("prev_burst_id", F.lag("burst_id", 1, 0).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("burst_id_bucket", F.ntile(3).over(w5))
    .withColumn("cumulative_session_id_2", F.sum("session_id_2").over(w6))
    .withColumn("event_type_count_session_id_2", F.count("session_id_2").over(w7))
)

result = (
    scored
    .select(
        "id",
        "session_id",
        "event_type",
        "session_start_flag",
        "row_seq",
        "burst_id_bucket",
        "cumulative_session_id_2",
        F.when(F.col("event_type").rlike("^(click|search)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("event_type_zone"),
    )
    .orderBy(F.col("event_type").desc(), F.col("burst_id_bucket").desc())
)
