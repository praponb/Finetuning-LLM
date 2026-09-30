from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter((F.col("a.status") == "OPEN") & F.month(F.col("c.signup_date")).isin([5, 8, 10]))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("a.account_type"),
        F.col("c.is_active"),
        F.col("c.age"),
    )
)
