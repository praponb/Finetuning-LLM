from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_first())
w3 = Window.partitionBy(F.date_trunc("week", "paid_at")).orderBy(F.col("amount").desc())
w4 = Window.partitionBy("order_id", "method").orderBy(F.col("amount_percent_rank").desc_nulls_first(), "id")
w5 = Window.partitionBy("method").orderBy(F.col("amount_rank").desc(), "id")
w_ordered = Window.partitionBy("order_id", "method").orderBy("amount_percent_rank", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("order_id")

latest = (
    df_payments
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest
    .filter(F.col("method") == "bank_transfer")
    .select(
        "id",
        "order_id",
        "method",
        F.rank().over(w2).alias("amount_rank"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    deduped
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("amount_percent_rank_avg_2_prior", F.avg("amount_percent_rank").over(w_ordered))
    .withColumn("order_id_stddev_samp_amount_rank", F.stddev_samp("amount_rank").over(w7))
)

base = (
    with_windows
    .filter(F.col("amount_percent_rank") <= 0.29)
    .select(
        "id",
        "event_seq",
        "order_id_stddev_samp_amount_rank",
        F.ceil("order_id_stddev_samp_amount_rank").alias("order_id_stddev_samp_amount_rank_whole"),
        F.round("event_seq", 0).alias("event_seq_rounded"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled"),
    )
)

valid = (
    base
    .filter(F.col("event_seq_rounded") >= 51)
    .select(
        "id",
        "event_seq",
        "order_id_stddev_samp_amount_rank",
        "event_seq_rounded",
        "method_filled",
        F.when(F.col("order_id_stddev_samp_amount_rank") >= 7, "High").when(F.col("order_id_stddev_samp_amount_rank") >= 2, "Medium").otherwise("Low").alias("order_id_stddev_samp_amount_rank_band"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        F.when(F.col("event_seq_rounded") >= 77, "PREMIUM").otherwise("STANDARD").alias("event_seq_rounded_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq_2"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.avg("order_id_stddev_samp_amount_rank").alias("avg_order_id_stddev_samp_amount_rank"),
    )
)
