from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_type_length").desc(), "id")
w2 = Window.partitionBy("session_id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("page").orderBy("event_time", "id")
w5 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy(F.col("event_type_length_avg_6_prior").desc(), "id")
w7 = Window.partitionBy("page", "event_type")

cleaned = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-04-14").cast("date"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.length("event_type").alias("event_type_length"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "event_type_length",
        F.ntile(10).over(w).alias("event_type_length_bucket"),
        F.min("duration_sec").over(w2).alias("session_id_min_duration_sec"),
        F.avg("event_type_length").over(w3).alias("event_type_length_avg_6_prior"),
    )
)

flagged = (
    scored
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "event_type_length_avg_6_prior",
        F.sum("is_new_burst").over(w5).alias("session_id_2"),
    )
)

with_windows = (
    sessionized
    .select(
        "*",
        F.row_number().over(w6).alias("event_seq"),
        F.stddev_samp("event_type_length_avg_6_prior").over(w7).alias("page_stddev_samp_event_type_length_avg_6_prior"),
    )
)

stats = (
    with_windows
    .groupBy("event_type")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.avg("session_id_2"), 2).alias("avg_session_id_2"),
    )
)

result = (
    stats
    .select(
        "event_type",
        "avg_session_id_2",
        F.when(F.col("event_type").rlike("^(add_to_cart|click)$"), "EXTENDED").when(F.col("event_type").rlike("^(page_view|purchase)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("event_type_zone"),
    )
    .orderBy("event_type_zone")
)
