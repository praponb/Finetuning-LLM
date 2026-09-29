from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.opened_date",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

result = (
    latest_match
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "balance", "opened_date", F.col("sum_account_balance"))
    .orderBy("sum_account_balance", F.col("balance").desc())
)
