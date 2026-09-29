from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("customer_id").isin([2, 15, 25]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.col("balance").cast("int").alias("balance_int"),
    )
)

base = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "balance_int",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance_filled",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        F.col("c.merchant"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("transaction_amount") >= 4300, "HIGH").otherwise("LOW").alias("transaction_amount_tier"),
    )
    .agg(
        F.round(F.avg("transaction_amount"), 2).alias("avg_transaction_amount"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
    .filter(F.col("savings_count") < 1000)
    .orderBy("transaction_amount_tier")
)
