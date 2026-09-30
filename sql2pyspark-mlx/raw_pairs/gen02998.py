from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("amount", "id")
w3 = Window.partitionBy("status")
w4 = Window.orderBy(F.col("prev_amount").desc_nulls_first())
w5 = Window.partitionBy("status").orderBy("seq_no", "id")
w6 = Window.partitionBy("status").orderBy("prev_amount_dense_rank", "id").rowsBetween(-3, -1)
w_grp = Window.orderBy("seq_no_2", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-07-09").cast("date"))
    .select(
        "id",
        "amount",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.substring("status", 1, 2).alias("status_prefix"),
        F.coalesce("method", F.lit("NONE")).alias("method_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
    )
)

enriched_payments = (
    enriched
    .filter(F.col("status") == "FAILED")
    .select(
        "*",
        F.dense_rank().over(w4).alias("prev_amount_dense_rank"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w3)), 4).alias("seq_no_share"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

with_windows = (
    enriched_payments
    .select(
        "id",
        "amount",
        "status_title",
        "status_prefix",
        "prev_amount",
        "prev_amount_dense_rank",
        "event_seq",
        F.row_number().over(w2).alias("seq_no_2"),
        F.max("seq_no").over(w3).alias("status_max_seq_no"),
        F.avg("prev_amount_dense_rank").over(w6).alias("prev_amount_dense_rank_avg_3_prior"),
    )
)

filtered_payments = (
    with_windows
    .filter(F.col("prev_amount_dense_rank") < 6)
    .select("*", F.least("prev_amount", "amount").alias("least_prev_amount_amount"))
)

result = (
    filtered_payments
    .select(
        "*",
        F.max("prev_amount_dense_rank_avg_3_prior").over(w_grp).alias("last4_max_prev_amount_dense_rank_avg_3_prior"),
        F.sum("prev_amount_dense_rank_avg_3_prior").over(w_grp).alias("last4_sum_prev_amount_dense_rank_avg_3_prior"),
        F.avg("prev_amount_dense_rank_avg_3_prior").over(w_grp).alias("last4_avg_prev_amount_dense_rank_avg_3_prior"),
        F.count("prev_amount_dense_rank_avg_3_prior").over(w_grp).alias("last4_count_prev_amount_dense_rank_avg_3_prior"),
    )
)
