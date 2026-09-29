from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy(F.year("opened_date"), "customer_id").orderBy(F.col("balance").desc())
w_part = Window.partitionBy("status").orderBy("balance_rank", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_accounts
    .filter((F.col("balance") > 33000) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.upper("status").alias("status_upper"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type").isin(["REFUND", "FEE"])), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

scored = (
    with_last
    .select(
        "id",
        "balance",
        "status",
        "status_upper",
        "transaction_amount",
        F.round((F.col("transaction_amount") / F.sum("transaction_amount").over(w2)), 4).alias("transaction_amount_share"),
        F.rank().over(w3).alias("balance_rank"),
    )
)

enriched = (
    scored
    .withColumn("last4_count_transaction_amount_share", F.count("transaction_amount_share").over(w_part))
    .withColumn("last4_min_transaction_amount_share", F.min("transaction_amount_share").over(w_part))
    .withColumn("last4_max_transaction_amount_share", F.max("transaction_amount_share").over(w_part))
)

result = (
    enriched
    .select(
        "id",
        "status",
        "status_upper",
        "transaction_amount",
        "balance_rank",
        "last4_min_transaction_amount_share",
    )
    .orderBy(F.col("last4_min_transaction_amount_share").asc_nulls_last())
)
