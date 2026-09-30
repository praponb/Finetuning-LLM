from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit").between(7800, 8000))
    .groupBy(
        F.when(F.col("c.credit_limit") >= 16600, "high").when(F.col("c.credit_limit") >= 4700, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
    )
    .agg(
        F.count(F.col("a.balance")).alias("balance_count"),
        F.avg(F.col("a.balance")).alias("avg_balance"),
    )
)
