from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy(F.date_trunc("YEAR", "event_time_day")).orderBy(F.col("duration_sec").desc(), "id")
w3 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec").desc_nulls_first())
w5 = Window.partitionBy("page", "event_type").orderBy(F.col("cumulative_duration_sec").desc(), "id")
w6 = Window.partitionBy("page").orderBy("duration_sec")
w7 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.year("event_time_day"), "page").orderBy("duration_sec_percent_rank", "id")
w9 = Window.partitionBy(F.date_trunc("MONTH", "event_time_day"), "page").orderBy("event_time_day", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.partitionBy("event_type").orderBy("duration_sec_avg_2_prior", "id")
w11 = Window.partitionBy("event_type").orderBy(F.col("cumulative_duration_sec").desc_nulls_last())
w12 = Window.partitionBy("event_type", "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w13 = Window.partitionBy(F.date_trunc("week", "event_time_day"), "event_type")
w14 = Window.partitionBy("page").orderBy("next_duration_sec_bucket", "id")
w15 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-03-13 06:00:00").cast("timestamp"))
    .select("*", F.col("event_time").cast("date").alias("event_time_day"))
)

enriched = (
    filtered
    .filter(F.col("event_time_day") > "2023-11-23")
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "event_time_day",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_2_prior"),
        F.ntile(3).over(w2).alias("duration_sec_bucket"),
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec"),
        F.percent_rank().over(w4).alias("duration_sec_percent_rank"),
    )
)

metrics = (
    enriched
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("duration_sec_cume_dist", F.cume_dist().over(w6))
    .withColumn("running_duration_sec_percent_rank", F.sum("duration_sec_percent_rank").over(w7))
    .withColumn("duration_sec_percent_rank_bucket", F.ntile(5).over(w8))
)

enriched_web_events = (
    metrics
    .withColumn("duration_sec_bucket_avg_6", F.avg("duration_sec_bucket").over(w9))
    .withColumn("next_duration_sec_bucket", F.lead("duration_sec_bucket", 1).over(w10))
    .withColumn("cumulative_duration_sec_rank", F.rank().over(w11))
    .withColumn("duration_sec_count_30d", F.count("duration_sec").over(w12))
)

enriched_web_events_2 = (
    enriched_web_events
    .withColumn("event_time_day_period_count_duration_sec_bucket", F.count("duration_sec_bucket").over(w13))
    .withColumn("event_seq", F.row_number().over(w14))
    .withColumn("next_duration_sec_bucket_sum_7d", F.sum("next_duration_sec_bucket").over(w15))
)

result = (
    enriched_web_events_2
    .select(
        "id",
        "page",
        "event_time",
        "event_time_day",
        "duration_sec_avg_2_prior",
        "duration_sec_percent_rank",
        "seq_no",
        "duration_sec_cume_dist",
        "duration_sec_percent_rank_bucket",
    )
)
