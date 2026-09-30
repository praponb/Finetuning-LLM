from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("t.id"),
        F.col("t.merchant"),
        F.col("t.account_id"),
        F.col("a.account_type"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("id").desc())
)
