from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy(F.col("session_start_flag").desc(), "id")

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-09-29").cast("date")) & F.col("page").isNotNull())
    .withColumn("clean_event_type", F.trim("event_type"))
)

gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

deduped = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    deduped
    .filter(F.col("page").isNotNull() & (F.col("duration_sec") >= 198))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "clean_event_type",
        "session_start_flag",
        "burst_id",
        F.year("event_time").alias("event_time_year"),
        F.coalesce("page", F.lit("Unknown")).alias("page_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "event_type",
        "event_time",
        "clean_event_type",
        "session_start_flag",
        "burst_id",
        "event_time_year",
        "page_filled",
    )
    .orderBy(F.col("event_type").desc(), F.col("event_time_year").desc())
)
