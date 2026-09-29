from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.coalesce("status", F.lit("Unknown")).alias("status_filled"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.status_filled",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

matched_keys = (
    with_last
    .select("customer_id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("customer_id")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") != "REFUND"), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("balance") >= 70000, "HIGH").otherwise("LOW").alias("balance_tier"),
        "transaction_txn_type",
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
        F.count(F.when(F.col("transaction_merchant") == "Lazada", 1)).alias("lazada_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
    )
)
