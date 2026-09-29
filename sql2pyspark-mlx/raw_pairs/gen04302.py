from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy(F.year("event_time_date"), "session_id")
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy("prev_duration_sec", "id").rowsBetween(-6, -1)
w_recent = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_last())
w6 = Window.partitionBy("event_type").orderBy("last7_avg_event_time_date_year_sum_duration_sec", "id")
w7 = Window.partitionBy("session_id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "*",
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_scaled"),
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "page_filled",
        F.to_date("event_time").alias("event_time_date"),
    )
)

enriched = (
    filtered
    .withColumn("prev_duration_sec", F.lag("duration_sec").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("event_time_date_year_sum_duration_sec", F.sum("duration_sec").over(w2))
)

with_windows = (
    enriched
    .select(
        "*",
        F.avg("event_time_date_year_sum_duration_sec").over(w3).alias("last7_avg_event_time_date_year_sum_duration_sec"),
        F.sum("event_time_date_year_sum_duration_sec").over(w3).alias("last7_sum_event_time_date_year_sum_duration_sec"),
        F.max("event_time_date_year_sum_duration_sec").over(w3).alias("last7_max_event_time_date_year_sum_duration_sec"),
        F.avg("event_seq").over(w4).alias("event_seq_avg_6_prior"),
    )
)

enriched_web_events = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("duration_sec_dense_rank"),
        F.ntile(10).over(w6).alias("last7_avg_event_time_date_year_sum_duration_sec_bucket"),
        F.sum("duration_sec").over(w7).alias("session_id_sum_duration_sec"),
    )
)

stats = (
    enriched_web_events
    .groupBy(F.date_trunc("YEAR", "event_time_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec_dense_rank"))).alias("page_view_total_duration_sec_dense_rank"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
)

result = (
    stats
    .select("year_start", "page_view_total_duration_sec_dense_rank", "num_event_types")
)
