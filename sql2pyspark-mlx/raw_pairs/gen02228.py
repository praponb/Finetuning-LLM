from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc_nulls_first(), "id")
w3 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy("rn", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy("event_time", "id")
w7 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("event_time") > F.to_date(F.lit("2023-10-23")))
    .withColumn("page_length", F.length("page"))
    .withColumn("page_event_type", F.concat("page", F.lit(" "), "event_type"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .filter(F.col("session_id") == 7)
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "page_event_type",
        "resolved_page",
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec"),
        F.max("duration_sec").over(w4).alias("duration_sec_max_7d"),
        F.avg("duration_sec").over(w5).alias("duration_sec_avg_6"),
    )
)

with_gaps = (
    metrics
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w6).isNull() | (F.col("event_time") > F.lag("event_time").over(w6) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w7))

with_gaps_2 = (
    islands
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w6) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands_web_events = with_gaps_2.withColumn("island_id", F.sum("new_group_flag").over(w7))

result = (
    islands_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "cumulative_duration_sec",
        "duration_sec_avg_6",
        "is_new_session",
        "session_id",
        "new_group_flag",
    )
    .orderBy(F.col("duration_sec_avg_6").desc_nulls_last())
)
