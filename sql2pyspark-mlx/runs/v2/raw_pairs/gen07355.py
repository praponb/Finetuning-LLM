from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type") != "LOAN")
    .select(
        F.col("t.merchant"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("c.segment"),
        F.col("c.country"),
    )
    .orderBy("opened_date")
)
