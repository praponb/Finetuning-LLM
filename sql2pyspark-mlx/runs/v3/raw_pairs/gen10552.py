from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 13700, "high").when(F.col("c.credit_limit") >= 6600, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.year(F.col("c.signup_date")).alias("signup_year"),
    )
    .agg(F.max(F.col("t.amount")).alias("max_amount"))
)
