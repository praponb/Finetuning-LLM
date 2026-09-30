from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("event_type")
w3 = Window.orderBy("last4_min_duration_sec", "id")
w4 = Window.orderBy("last4_sum_duration_sec", "id").rowsBetween(-3, -1)
w5 = Window.orderBy(F.col("next_last4_sum_duration_sec").asc_nulls_last())
w6 = Window.orderBy("event_type_count_last4_sum_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last4_sum_duration_sec", "id")
w8 = Window.orderBy("first_known_event_type_count_last4_sum_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("event_type_count_last4_sum_duration_sec_avg_3_prior").desc_nulls_first())
w10 = Window.orderBy(F.col("event_type_count_last4_sum_duration_sec").desc(), "id")
w11 = Window.orderBy(F.col("last4_sum_duration_sec").desc(), "id")
w12 = Window.orderBy("last4_sum_duration_sec_bucket_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("event_seq", "id").rowsBetween(-2, Window.currentRow)
w14 = Window.orderBy("first_known_last4_sum_duration_sec_bucket_2", "id")
w15 = Window.orderBy("last4_sum_duration_sec_bucket", "id").rowsBetween(-5, Window.currentRow)
w16 = Window.orderBy(F.col("event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist").desc(), "id")
w17 = Window.orderBy("first_known_last4_sum_duration_sec_bucket", "id").rowsBetween(-6, -1)

scored = (
    df_web_events
    .select(
        "id",
        "event_type",
        F.count("duration_sec").over(w).alias("last4_count_duration_sec"),
        F.min("duration_sec").over(w).alias("last4_min_duration_sec"),
        F.sum("duration_sec").over(w).alias("last4_sum_duration_sec"),
    )
)

with_windows = (
    scored
    .filter(F.col("last4_min_duration_sec") <= 280)
    .select(
        "id",
        "last4_count_duration_sec",
        "last4_sum_duration_sec",
        F.count("last4_sum_duration_sec").over(w2).alias("event_type_count_last4_sum_duration_sec"),
        F.lead("last4_sum_duration_sec", 1).over(w3).alias("next_last4_sum_duration_sec"),
    )
)

metrics = (
    with_windows
    .withColumn("event_type_count_last4_sum_duration_sec_avg_3_prior", F.avg("event_type_count_last4_sum_duration_sec").over(w4))
    .withColumn("next_last4_sum_duration_sec_dense_rank", F.dense_rank().over(w5))
    .withColumn("first_known_event_type_count_last4_sum_duration_sec", F.first("event_type_count_last4_sum_duration_sec", ignorenulls=True).over(w6))
    .withColumn("last4_sum_duration_sec_bucket", F.ntile(3).over(w7))
    .withColumn("last4_sum_duration_sec_bucket_2", F.ntile(5).over(w7))
)

with_windows_web_events = (
    metrics
    .select(
        "id",
        "last4_sum_duration_sec",
        "next_last4_sum_duration_sec",
        "last4_sum_duration_sec_bucket",
        F.sum("last4_count_duration_sec").over(w8).alias("cumulative_last4_count_duration_sec"),
        F.cume_dist().over(w9).alias("event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist"),
        F.row_number().over(w10).alias("event_seq"),
        F.ntile(5).over(w11).alias("last4_sum_duration_sec_bucket_3"),
        F.first("last4_sum_duration_sec_bucket", ignorenulls=True).over(w6).alias("first_known_last4_sum_duration_sec_bucket"),
    )
)

metrics_web_events = (
    with_windows_web_events
    .select(
        "*",
        F.first("last4_sum_duration_sec_bucket", ignorenulls=True).over(w12).alias("first_known_last4_sum_duration_sec_bucket_2"),
        F.avg("event_seq").over(w13).alias("event_seq_avg_2"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

with_windows_web_events_2 = (
    metrics_web_events
    .select(
        "*",
        F.lead("event_seq", 1).over(w14).alias("next_event_seq"),
        F.ntile(10).over(w7).alias("last4_sum_duration_sec_bucket_2"),
        F.avg("last4_sum_duration_sec_bucket_3").over(w15).alias("last4_sum_duration_sec_bucket_3_avg_5"),
        F.row_number().over(w16).alias("seq_no"),
        F.avg("last4_sum_duration_sec_bucket_3").over(w17).alias("last4_sum_duration_sec_bucket_3_avg_6_prior"),
    )
)

filtered = (
    with_windows_web_events_2
    .filter(F.col("last4_sum_duration_sec_bucket_3_avg_6_prior").between(4, 7))
    .select(
        "id",
        "last4_sum_duration_sec",
        "next_last4_sum_duration_sec",
        "last4_sum_duration_sec_bucket",
        "cumulative_last4_count_duration_sec",
        "event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist",
        "event_seq",
        "last4_sum_duration_sec_bucket_3",
        "event_seq_avg_2",
        "next_event_seq",
        "last4_sum_duration_sec_bucket_2",
        "last4_sum_duration_sec_bucket_3_avg_5",
        "seq_no",
        "last4_sum_duration_sec_bucket_3_avg_6_prior",
        F.round("next_event_seq", 2).alias("next_event_seq_rounded"),
    )
)

result = (
    filtered
    .groupBy(
        "seq_no",
        F.when(F.col("event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist") >= 0.8, "LARGE").otherwise("SMALL").alias("event_type_count_last4_sum_duration_sec_avg_3_prior_cume_dist_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids_2") > 1)
)
