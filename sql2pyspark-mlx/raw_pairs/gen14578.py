from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "event_time"), "page").orderBy(F.col("duration_sec").asc_nulls_first())
w3 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("duration_sec_cume_dist").desc(), "id")
w_ordered = Window.orderBy(F.col("running_min_duration_sec").asc_nulls_first())
w6 = Window.orderBy(F.col("duration_sec").desc(), "id")
w7 = Window.orderBy(F.col("duration_sec_cume_dist_bucket_adjusted").desc_nulls_first())
w8 = Window.orderBy(F.col("running_avg_duration_sec").asc_nulls_first())

metrics = (
    df_web_events
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.avg("duration_sec").over(w).alias("running_avg_duration_sec"),
        F.min("duration_sec").over(w).alias("running_min_duration_sec"),
        F.max("duration_sec").over(w).alias("running_max_duration_sec"),
        F.cume_dist().over(w2).alias("duration_sec_cume_dist"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "duration_sec",
        "running_avg_duration_sec",
        "running_min_duration_sec",
        "running_max_duration_sec",
        F.sum("duration_sec").over(w3).alias("running_sum_duration_sec"),
        F.count("duration_sec").over(w3).alias("running_count_duration_sec"),
        F.max("duration_sec").over(w3).alias("running_max_duration_sec_2"),
        F.ntile(4).over(w4).alias("duration_sec_cume_dist_bucket"),
    )
)

cleaned = (
    enriched
    .filter(F.col("duration_sec_cume_dist_bucket").isNotNull())
    .select(
        "*",
        F.round((F.col("duration_sec_cume_dist_bucket") * 1.15), 2).alias("duration_sec_cume_dist_bucket_adjusted"),
    )
)

scored = (
    cleaned
    .filter(F.col("running_sum_duration_sec").between(1300, 1400))
    .select(
        "id",
        "running_avg_duration_sec",
        "running_count_duration_sec",
        "duration_sec_cume_dist_bucket_adjusted",
        F.cume_dist().over(w_ordered).alias("running_min_duration_sec_cume_dist"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

filtered = (
    scored
    .filter(F.col("running_avg_duration_sec") >= 330)
    .withColumn("running_count_duration_sec_times_running_avg_duration_sec", (F.col("running_count_duration_sec") * F.col("running_avg_duration_sec")))
)

with_windows = (
    filtered
    .withColumn("duration_sec_cume_dist_bucket_adjusted_rank", F.rank().over(w7))
    .withColumn("running_avg_duration_sec_percent_rank", F.percent_rank().over(w8))
)

cleaned_web_events = (
    with_windows
    .filter(F.col("duration_sec_cume_dist_bucket_adjusted") < 21)
    .select(
        "*",
        (F.col("running_avg_duration_sec") * F.col("row_seq")).alias("running_avg_duration_sec_times_row_seq"),
    )
)

stats = (
    cleaned_web_events
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("running_avg_duration_sec_percent_rank_tier", F.when(F.col("running_avg_duration_sec_percent_rank") >= 0.84, "LARGE").otherwise("SMALL"))
    .rollup("row_seq_tier", "running_avg_duration_sec_percent_rank_tier")
    .agg(
        F.sum("running_avg_duration_sec_percent_rank").alias("total_running_avg_duration_sec_percent_rank"),
        F.count("*").alias("row_count"),
        F.round(F.avg("duration_sec_cume_dist_bucket_adjusted_rank"), 2).alias("avg_duration_sec_cume_dist_bucket_adjusted_rank"),
        F.avg("running_min_duration_sec_cume_dist").alias("avg_running_min_duration_sec_cume_dist"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = stats.select("row_count", "avg_duration_sec_cume_dist_bucket_adjusted_rank")
