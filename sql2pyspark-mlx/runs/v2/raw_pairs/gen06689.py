from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "right")
    .filter((F.col("a.account_type") != "LOAN") & (F.col("a.balance") < 35000))
    .select(
        F.col("t.amount"),
        F.col("a.account_type"),
        F.col("c.name"),
        F.add_months(F.col("a.opened_date"), 6).alias("opened_date_plus_months"),
    )
)
