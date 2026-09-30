from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type")
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy("amount", "id")
w6 = Window.partitionBy("merchant").orderBy("prev_amount_max_90d_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.month("txn_time"), "merchant").orderBy("amount_bucket", "id")

with_windows = (
    df_transactions
    .withColumn("amount_bucket", F.ntile(3).over(w))
    .withColumn("txn_type_avg_amount", F.avg("amount").over(w2))
    .withColumn("amount_max_90d", F.max("amount").over(w3))
    .withColumn("amount_max_90d_2", F.max("amount").over(w3))
    .withColumn("cumulative_amount", F.sum("amount").over(w4))
)

metrics = (
    with_windows
    .filter(F.col("account_id").isin([6, 28, 40]))
    .select(
        "*",
        F.lag("amount").over(w5).alias("prev_amount"),
        F.lead("amount").over(w5).alias("next_amount"),
        F.lag("amount_max_90d_2").over(w5).alias("prev_amount_max_90d_2"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

base = (
    metrics
    .filter(F.col("merchant").isNotNull() & (F.col("prev_amount") >= 3000))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_bucket",
        "amount_max_90d",
        "amount_max_90d_2",
        "cumulative_amount",
        "prev_amount",
        "next_amount",
        "prev_amount_max_90d_2",
        "row_seq",
        F.coalesce("amount_bucket", F.lit(0)).alias("amount_bucket_filled"),
    )
)

filtered = (
    base
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_bucket",
        "amount_max_90d",
        "amount_max_90d_2",
        "cumulative_amount",
        "prev_amount",
        "next_amount",
        "prev_amount_max_90d_2",
        "row_seq",
        "amount_bucket_filled",
        F.concat_ws("/", "txn_type", "merchant").alias("txn_type_merchant_key"),
        F.initcap(F.trim("merchant")).alias("merchant_title"),
    )
)

with_windows_transactions = (
    filtered
    .select(
        "id",
        "txn_time",
        "is_flagged",
        "amount_max_90d",
        "amount_max_90d_2",
        "cumulative_amount",
        "next_amount",
        "row_seq",
        "amount_bucket_filled",
        "txn_type_merchant_key",
        F.avg("amount").over(w6).alias("running_avg_amount"),
        F.max("amount").over(w6).alias("running_max_amount"),
        F.count("amount").over(w6).alias("running_count_amount"),
        F.min("amount").over(w6).alias("running_min_amount"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

result = (
    with_windows_transactions
    .select(
        "id",
        "txn_time",
        "is_flagged",
        "amount_max_90d",
        "amount_max_90d_2",
        "cumulative_amount",
        "next_amount",
        "amount_bucket_filled",
        "running_max_amount",
    )
    .orderBy(F.col("amount_max_90d_2").desc_nulls_last(), "id")
)
