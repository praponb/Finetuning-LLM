from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").asc_nulls_first(), "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(~F.col("page").eqNullSafe(F.lit("/home")))
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("page_event_type", F.concat("page", F.lit(" - "), "event_type"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped_web_events = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

with_gaps = (
    deduped_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id_2", F.sum("is_new_session").over(w4))

result = (
    islands
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        "page_title",
        "page_event_type",
        "resolved_page",
        "dedup_rank",
    )
    .orderBy(F.col("duration_sec").desc())
)
