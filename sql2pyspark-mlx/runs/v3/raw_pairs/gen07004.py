from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14100, "high").when(F.col("c.credit_limit") >= 2900, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.country"),
    )
    .agg(F.count(F.when(F.col("a.account_type") == "LOAN", 1)).alias("loan_count"))
    .filter(F.col("loan_count") > 4)
)
