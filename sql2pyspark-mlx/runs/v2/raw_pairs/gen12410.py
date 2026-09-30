from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "outer")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 15300, "high").when(F.col("c.credit_limit") >= 4900, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.max(F.col("a.opened_date")).alias("last_opened_date"),
        F.avg(F.col("a.balance")).alias("avg_balance"),
    )
)
