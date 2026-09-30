from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.to_date("event_time"))
w4 = Window.partitionBy(F.to_date("event_time")).orderBy(F.col("duration_sec").desc_nulls_first())
w_recent = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.orderBy(F.col("running_count_duration_sec_rounded").desc_nulls_last())

ranked = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "event_time", "duration_sec")
)

with_windows = (
    ranked
    .withColumn("running_sum_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("running_count_duration_sec", F.count("duration_sec").over(w2))
    .withColumn("running_max_duration_sec", F.max("duration_sec").over(w2))
)

remaining_keys = (
    with_windows
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec") >= 285)
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-05-14")))
    .select(
        "event_time",
        "duration_sec",
        "running_count_duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
)

valid = (
    cleaned
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-05-26")))
    .select(
        "*",
        F.round("running_count_duration_sec", 2).alias("running_count_duration_sec_rounded"),
    )
)

result = (
    valid
    .select(
        "duration_sec",
        "running_count_duration_sec",
        "duration_sec_whole",
        F.max("duration_sec_whole").over(w_ordered).alias("event_time_to_date_max_duration_sec_whole"),
        F.cume_dist().over(w4).alias("duration_sec_cume_dist"),
        F.avg("duration_sec_whole").over(w_recent).alias("duration_sec_whole_avg_90d"),
        F.sum("running_count_duration_sec_rounded").over(w6).alias("running_count_duration_sec_rounded_sum_7d"),
        F.percent_rank().over(w7).alias("running_count_duration_sec_rounded_percent_rank"),
    )
)
