from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("customer_id").orderBy("balance", "id")
w5 = Window.partitionBy("account_type").orderBy("first_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

enriched = (
    df_accounts
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w))
    .withColumn("first_known_balance_2", F.first("balance", ignorenulls=True).over(w2))
)

with_windows = (
    enriched
    .withColumn("status_stddev_samp_first_known_balance_2", F.stddev_samp("first_known_balance_2").over(w3))
    .withColumn("balance_bucket", F.ntile(4).over(w4))
    .withColumn("running_balance", F.sum("balance").over(w5))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

latest_match_accounts = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

remaining_keys = (
    latest_match_accounts
    .select("account_type", "id")
    .exceptAll(
        df_accounts
        .filter(F.col("balance").between(44000, 58000))
        .select("account_type", "id")
    )
)

reconciled = (
    latest_match_accounts
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "first_known_balance",
        "status_stddev_samp_first_known_balance_2",
        "balance_bucket",
        "running_balance",
    )
    .orderBy(F.col("balance").desc_nulls_last())
)
