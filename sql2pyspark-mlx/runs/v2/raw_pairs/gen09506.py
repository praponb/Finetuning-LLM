from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("t.amount").isNotNull())
    .groupBy(F.month(F.col("a.opened_date")).alias("opened_month"))
    .agg((F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"))
)
