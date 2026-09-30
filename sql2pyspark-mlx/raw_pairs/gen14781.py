from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("event_time", "id")
w5 = Window.partitionBy("device").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("event_time", "id")
w7 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_web_events
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    latest
    .filter(F.col("page").isNotNull() & (~F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "*",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w3))

enriched = (
    islands
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.event_time",
        "s.duration_sec",
        "s.burst_id",
        F.col("b.device"),
    )
)

flagged = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("island_id", F.sum("is_new_burst").over(w5))

flagged_2 = (
    sessions_web_events
    .withColumn("is_new_burst_2", F.when(F.col("event_time") - F.lag("event_time").over(w6) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged_2
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "is_new_burst",
        F.sum("is_new_burst_2").over(w7).alias("burst_id_2"),
    )
)

result = (
    sessionized
    .groupBy("page")
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("page") == "/home", 1)), F.lit(0)), 2).alias("is_new_burst_per_home"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("row_count"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("span_minutes").asc_nulls_last())
)
