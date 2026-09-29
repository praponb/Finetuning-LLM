from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.city"),
        F.when(F.col("c.credit_limit") >= 13200, "high").when(F.col("c.credit_limit") >= 2500, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") >= 75)
)
