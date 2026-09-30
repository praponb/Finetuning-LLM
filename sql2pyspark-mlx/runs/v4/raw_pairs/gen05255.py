from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy("duration_sec")
w5 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy(F.col("duration_sec_avg_2_prior").asc_nulls_last())
w7 = Window.partitionBy("event_type")
w8 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w9 = Window.orderBy("duration_sec_avg_5_prior_sum_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("page").orderBy("duration_sec_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("page").orderBy("duration_sec_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w13 = Window.orderBy("duration_sec_avg_2_prior_percent_rank", "id")
w14 = Window.partitionBy("event_type").orderBy(F.col("distinct_ids").desc())

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-10-26 23:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "*",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_5_prior"),
        F.avg("duration_sec").over(w2).alias("duration_sec_avg_2_prior"),
        F.sum("duration_sec").over(w3).alias("running_duration_sec"),
        F.dense_rank().over(w4).alias("duration_sec_dense_rank"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.last("duration_sec_avg_5_prior", ignorenulls=True).over(w5).alias("last_known_duration_sec_avg_5_prior"),
        F.percent_rank().over(w6).alias("duration_sec_avg_2_prior_percent_rank"),
        F.count("duration_sec_dense_rank").over(w7).alias("event_type_count_duration_sec_dense_rank"),
        F.sum("running_duration_sec").over(w3).alias("cumulative_running_duration_sec"),
        F.sum("duration_sec_avg_5_prior").over(w8).alias("duration_sec_avg_5_prior_sum_7d"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.first("duration_sec_avg_2_prior_percent_rank", ignorenulls=True).over(w9).alias("first_known_duration_sec_avg_2_prior_percent_rank"),
        F.first("last_known_duration_sec_avg_5_prior", ignorenulls=True).over(w10).alias("first_known_last_known_duration_sec_avg_5_prior"),
        F.first("duration_sec_dense_rank", ignorenulls=True).over(w11).alias("first_known_duration_sec_dense_rank"),
        F.max("duration_sec_avg_2_prior").over(w12).alias("duration_sec_avg_2_prior_max_90d"),
        F.lead("duration_sec_avg_2_prior_percent_rank", 2, 0).over(w13).alias("next_duration_sec_avg_2_prior_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec_avg_2_prior_max_90d") >= 461, "PREMIUM").otherwise("STANDARD").alias("duration_sec_avg_2_prior_max_90d_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("event_time").alias("first_event_time"),
    )
    .withColumn("distinct_ids_rank", F.rank().over(w14))
    .orderBy("distinct_ids")
)
