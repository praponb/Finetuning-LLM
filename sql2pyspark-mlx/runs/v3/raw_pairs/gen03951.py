from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("t.merchant"),
        F.col("t.txn_type"),
        F.col("a.opened_date"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.add_months(F.col("a.opened_date"), 12).alias("opened_date_plus_months"),
    )
)
