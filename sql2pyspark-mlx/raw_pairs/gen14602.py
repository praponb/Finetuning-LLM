from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("account_id").orderBy("amount_adjusted", "id")
w2 = Window.partitionBy("merchant", "account_id")
w3 = Window.partitionBy("amount_tier").orderBy(F.col("debit_max_prev_amount_minus_amount_adjusted").desc())

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= "2024-02-16"))
    .select(
        "*",
        F.round((F.col("amount") * 1.15), 2).alias("amount_adjusted"),
        F.when(F.col("merchant").isin(["Starbucks", "Shell"]), "Priority").otherwise("Other").alias("merchant_group"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "amount_adjusted",
        F.lag("amount_adjusted").over(w_ordered).alias("prev_amount_adjusted"),
        F.lag("amount").over(w_ordered).alias("prev_amount"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.sum("amount").over(w2).alias("merchant_sum_amount"),
    )
)

base = (
    enriched
    .filter(F.col("amount_adjusted") < 27)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "is_flagged",
        "amount_adjusted",
        "prev_amount_adjusted",
        "prev_amount",
        "merchant_sum_amount",
        (F.col("prev_amount") - F.col("amount_adjusted")).alias("prev_amount_minus_amount_adjusted"),
    )
)

remaining_keys = (
    base
    .select("txn_type", "account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_time") < "2025-03-09")
        .select("txn_type", "account_id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("amount") >= 3500, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.when(F.col("prev_amount") >= 3800, "LARGE").otherwise("SMALL").alias("prev_amount_tier"),
        "account_id",
    )
    .agg(
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.max(F.when(F.col("txn_type") == "DEBIT", F.col("prev_amount_minus_amount_adjusted"))).alias("debit_max_prev_amount_minus_amount_adjusted"),
    )
    .filter(F.col("debit_max_prev_amount_minus_amount_adjusted") > 0)
    .withColumn("debit_max_prev_amount_minus_amount_adjusted_rank", F.rank().over(w3))
    .orderBy(F.col("distinct_txn_types").desc())
)
