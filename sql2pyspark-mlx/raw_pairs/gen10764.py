from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter((F.col("t.txn_time") < "2024-02-01") & (F.col("a.opened_date") >= "2023-07-01"))
    .select(F.col("t.account_id"), F.col("a.account_type"), F.col("c.name"))
    .orderBy("account_type")
)
