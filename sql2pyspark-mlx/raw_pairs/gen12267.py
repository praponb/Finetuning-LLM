from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w_running = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id")
w4 = Window.partitionBy("merchant").orderBy(F.col("next_amount").desc_nulls_first(), "id")
w5 = Window.partitionBy("merchant").orderBy(F.col("amount_dense_rank").asc_nulls_last(), "id")
w6 = Window.partitionBy("merchant").orderBy(F.col("rank_in_group").desc(), "id")
w_hist = Window.orderBy(F.col("prev_amount").desc(), "id")
w8 = Window.partitionBy("merchant").orderBy("next_amount", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("merchant").orderBy("dedup_rank", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.partitionBy("merchant").orderBy("next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.orderBy(F.col("prev_amount").desc())

metrics = (
    df_transactions
    .select(
        "id",
        "merchant",
        "is_flagged",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.lead("amount", 2, 0).over(w_running).alias("next_amount"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

ranked_transactions = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked_transactions
    .withColumn("prev_amount_bucket", F.ntile(10).over(w_hist))
    .withColumn("next_amount_avg_5_prior", F.avg("next_amount").over(w8))
    .withColumn("amount_dense_rank_avg_5", F.avg("amount_dense_rank").over(w9))
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w10))
    .withColumn("prev_amount_rank", F.rank().over(w_hist_2))
)

result = (
    enriched
    .select(
        "id",
        "merchant",
        "is_flagged",
        "amount_dense_rank",
        "prev_amount",
        "next_amount",
        "rank_in_group",
        "latest_rank",
        "prev_amount_bucket",
    )
)
