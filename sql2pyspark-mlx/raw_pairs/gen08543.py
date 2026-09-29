from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 17800, "high").when(F.col("c.credit_limit") >= 4900, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.city"),
    )
    .agg(
        F.avg(F.col("a.balance")).alias("avg_balance"),
        F.countDistinct(F.col("a.balance")).alias("unique_customers"),
    )
    .filter(F.col("avg_balance") > 78000)
    .orderBy(F.col("avg_balance").desc())
)
