from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("a.balance") < 32000) | F.col("t.account_id").isin([1, 8, 33, 38]))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.merchant"),
        F.col("a.opened_date"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
    )
)
