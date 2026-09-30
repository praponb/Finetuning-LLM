from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .withColumn("status_filled", F.coalesce("status", F.lit("Unknown")))
)

with_last = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.status",
        "s.status_filled",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type") == "DEBIT"), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.status_filled",
        "s.transaction_txn_type",
        F.col("c.txn_type"),
    )
)

result = (
    latest_match
    .groupBy("transaction_txn_type")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.sort_array(F.collect_set("transaction_txn_type")).alias("transaction_txn_type_list"),
        F.count(F.when(F.col("transaction_txn_type") == "FEE", 1)).alias("fee_rows"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
    .filter(F.col("fee_rows") >= 1)
)
