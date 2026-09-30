from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant")
w2 = Window.partitionBy("account_id", "merchant").orderBy(F.col("adjusted_amount").desc(), "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-3, -1)
w5 = Window.orderBy("adjusted_amount", "id")
w6 = Window.partitionBy("txn_type", "merchant").orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type")
w8 = Window.partitionBy(F.year("txn_time"), "merchant")
w_part = Window.partitionBy("merchant").orderBy(F.col("amount_avg_3_prior").desc_nulls_first())
w10 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w11 = Window.orderBy("running_min_amount_avg_3_prior_avg_7d")
w12 = Window.orderBy("first_known_amount")
w13 = Window.orderBy(F.col("amount_avg_3_prior").desc())

valid = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Shell")))
    .withColumn("adjusted_amount", F.round((F.col("amount") * 0.9), 2))
)

enriched = (
    valid
    .filter(F.col("txn_type").isin(["DEBIT", "CREDIT", "REFUND"]))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.row_number().over(w2).alias("row_seq"),
        F.first("amount", ignorenulls=True).over(w3).alias("first_known_amount"),
        F.avg("amount").over(w4).alias("amount_avg_3_prior"),
        F.lead("amount", 1).over(w5).alias("next_amount"),
    )
)

scored = (
    enriched
    .withColumn("running_min_amount_avg_3_prior", F.min("amount_avg_3_prior").over(w6))
    .withColumn("running_avg_amount_avg_3_prior", F.avg("amount_avg_3_prior").over(w6))
    .withColumn("running_max_amount_avg_3_prior", F.max("amount_avg_3_prior").over(w6))
    .withColumn("running_sum_amount_avg_3_prior", F.sum("amount_avg_3_prior").over(w6))
)

matched_keys = (
    scored
    .select("txn_type", "id")
    .exceptAll(
        df_transactions
        .filter(F.col("txn_type") == "DEBIT")
        .select("txn_type", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "txn_time",
        "merchant",
        "amount_share",
        "first_known_amount",
        "amount_avg_3_prior",
        "next_amount",
        "running_avg_amount_avg_3_prior",
        "running_max_amount_avg_3_prior",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w7)), 4).alias("row_seq_share"),
        F.round((F.col("next_amount") / F.sum("next_amount").over(w8)), 4).alias("next_amount_share"),
        F.dense_rank().over(w_part).alias("amount_avg_3_prior_dense_rank"),
        F.avg("running_min_amount_avg_3_prior").over(w10).alias("running_min_amount_avg_3_prior_avg_7d"),
    )
)

with_windows_transactions = (
    with_windows
    .select(
        "txn_time",
        "first_known_amount",
        "amount_avg_3_prior",
        "next_amount",
        "running_avg_amount_avg_3_prior",
        "next_amount_share",
        "amount_avg_3_prior_dense_rank",
        F.cume_dist().over(w11).alias("running_min_amount_avg_3_prior_avg_7d_cume_dist"),
    )
)

result = (
    with_windows_transactions
    .select(
        "first_known_amount",
        "next_amount",
        "running_avg_amount_avg_3_prior",
        F.rank().over(w12).alias("first_known_amount_rank"),
        F.percent_rank().over(w13).alias("amount_avg_3_prior_percent_rank"),
    )
)
