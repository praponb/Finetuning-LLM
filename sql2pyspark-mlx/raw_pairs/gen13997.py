from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy(F.month("txn_time"), "txn_type").orderBy(F.col("amount").desc_nulls_last())
w_hist = Window.partitionBy(F.month("txn_time"), "merchant").orderBy("txn_time", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy(F.month("txn_time"), "merchant").orderBy(F.col("amount_bucket").desc())
w6 = Window.partitionBy(F.month("txn_time"), "account_id").orderBy(F.col("amount_avg_6_prior").desc_nulls_last())
w7 = Window.orderBy(F.col("amount").desc())
w8 = Window.partitionBy(F.date_trunc("WEEK", "txn_time"), "txn_type").orderBy(F.col("amount_avg_6_prior").asc_nulls_first())
w9 = Window.orderBy("amount_cume_dist")

deduped = (
    df_transactions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    deduped
    .withColumn("amount_bucket", F.ntile(4).over(w2))
    .withColumn("amount_cume_dist", F.cume_dist().over(w3))
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w_hist))
)

remaining_keys = (
    scored
    .select("account_id", "id")
    .subtract(
        df_transactions
        .filter(F.col("amount") >= 1000)
        .select("account_id", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("merchant", "txn_type")
    .exceptAll(
        df_transactions
        .filter(F.col("amount").between(1900, 2000))
        .select("merchant", "txn_type")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .filter(F.col("merchant").isin(["Lazada", "Starbucks"]))
    .select(
        "is_flagged",
        "amount_bucket",
        F.cume_dist().over(w5).alias("amount_bucket_cume_dist"),
        F.percent_rank().over(w6).alias("amount_avg_6_prior_percent_rank"),
        F.percent_rank().over(w7).alias("amount_percent_rank"),
        F.rank().over(w8).alias("amount_avg_6_prior_rank"),
        F.rank().over(w9).alias("amount_cume_dist_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("amount_avg_6_prior_rank") <= 8)
    .select(
        "*",
        F.coalesce("amount_bucket_cume_dist", F.lit(0)).alias("amount_bucket_cume_dist_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("amount_cume_dist_rank") < 3)
    .withColumn("amount_bucket_cume_dist_filled_adjusted", F.round((F.col("amount_bucket_cume_dist_filled") * 1.15), 2))
)

result = (
    base
    .select(
        "amount_bucket_cume_dist",
        "amount_percent_rank",
        "amount_cume_dist_rank",
        "amount_bucket_cume_dist_filled",
    )
)
