from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("t.txn_time") < "2023-12-08")
    .select(
        F.col("t.is_flagged"),
        F.col("a.status"),
        F.col("c.country"),
        F.col("c.age"),
        F.date_trunc("month", F.col("a.opened_date")).alias("opened_date_period"),
    )
)
