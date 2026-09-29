from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy(F.to_date("txn_time"), "merchant").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("txn_type").orderBy("balance", "id")
w4 = Window.partitionBy("txn_type").orderBy("balance")
w5 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last(), "id")
w6 = Window.partitionBy("txn_type", "account_type").orderBy("balance", "id").rowsBetween(-2, -1)
w7 = Window.orderBy("balance", "id")
w8 = Window.partitionBy("txn_type").orderBy("balance_bucket", "id")
w9 = Window.partitionBy("account_type")
w10 = Window.partitionBy("txn_type")
w11 = Window.orderBy(F.col("seq_no").desc_nulls_last())

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.balance"), F.col("d.account_type"))
)

scored = (
    with_dims
    .select(
        "*",
        F.avg("balance").over(w_part).alias("balance_avg_3_prior"),
        F.sum("balance").over(w2).alias("running_balance"),
        F.ntile(3).over(w_recent).alias("balance_bucket"),
        F.percent_rank().over(w4).alias("balance_percent_rank"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "is_flagged_label",
        "merchant_clean",
        "balance",
        "account_type",
        "running_balance",
        "balance_bucket",
    )
)

scored_transactions = (
    ranked
    .withColumn("balance_avg_2_prior", F.avg("balance").over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("prev_balance", F.lag("balance", 2, 0).over(w8))
    .withColumn("account_type_min_balance_bucket", F.min("balance_bucket").over(w9))
)

metrics = (
    scored_transactions
    .select(
        "id",
        "is_flagged",
        "merchant_clean",
        "balance",
        "balance_avg_2_prior",
        "seq_no",
        "prev_balance",
        "account_type_min_balance_bucket",
        F.round((F.col("balance_bucket") / F.sum("balance_bucket").over(w10)), 4).alias("balance_bucket_share"),
        F.cume_dist().over(w11).alias("seq_no_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "is_flagged",
        "merchant_clean",
        "balance",
        "seq_no",
        "account_type_min_balance_bucket",
    )
)
