from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_type_length", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("page", "event_type").orderBy(F.col("last7_min_duration_sec").desc_nulls_first())

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/checkout"))) & (F.col("event_time") >= "2025-09-14"))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        F.length("event_type").alias("event_type_length"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.min("duration_sec").over(w).alias("last7_min_duration_sec"),
        F.avg("duration_sec").over(w).alias("last7_avg_duration_sec"),
        F.max("duration_sec").over(w).alias("last7_max_duration_sec"),
        F.sum("duration_sec").over(w).alias("last7_sum_duration_sec"),
    )
)

base = (
    with_windows
    .filter((~F.col("page").eqNullSafe(F.lit("/checkout"))) & (F.col("last7_sum_duration_sec") <= 1500))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "event_type_length",
        "page_filled",
        "last7_min_duration_sec",
        "last7_max_duration_sec",
        "last7_sum_duration_sec",
        (F.col("last7_min_duration_sec") / F.nullif("last7_max_duration_sec", F.lit(0))).alias("last7_min_duration_sec_per_last7_max_duration_sec"),
    )
)

latest = (
    base
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "page",
        "event_type_length",
        "page_filled",
        "last7_min_duration_sec",
        "last7_max_duration_sec",
        "last7_sum_duration_sec",
        "last7_min_duration_sec_per_last7_max_duration_sec",
        "row_num",
    )
    .orderBy("event_type_length")
)
