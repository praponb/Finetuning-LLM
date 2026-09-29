from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("account_id", "txn_type").orderBy("running_min_amount")
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_first())
w5 = Window.partitionBy(F.month("txn_time"), "account_id").orderBy(F.col("running_min_amount").desc_nulls_last())
w6 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-08-08").cast("date"))
    .withColumn("txn_time_day", F.col("txn_time").cast("date"))
    .withColumn("clean_merchant", F.trim("merchant"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("UNKNOWN")))
)

enriched = (
    valid
    .filter(F.col("txn_time_day") >= "2024-09-13")
    .select(
        "*",
        F.count("amount").over(w_ordered).alias("running_count_amount"),
        F.min("amount").over(w_ordered).alias("running_min_amount"),
        F.sum("amount").over(w_ordered).alias("running_sum_amount"),
        F.avg("amount").over(w2).alias("amount_avg_2"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "merchant_filled",
        "running_min_amount",
        "amount_avg_2",
        "row_num",
        F.dense_rank().over(w4).alias("amount_dense_rank"),
        F.rank().over(w5).alias("running_min_amount_rank"),
        F.last("running_min_amount", ignorenulls=True).over(w6).alias("last_known_running_min_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "is_flagged",
        "merchant_filled",
        "running_min_amount",
        "amount_dense_rank",
        "running_min_amount_rank",
        "last_known_running_min_amount",
    )
    .orderBy(F.col("running_min_amount_rank").desc_nulls_last(), "id")
)
