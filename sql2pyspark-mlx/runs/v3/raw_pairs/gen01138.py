from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.account_type") == "LOAN")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14100, "high").when(F.col("c.credit_limit") >= 6800, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.segment"),
    )
    .agg(F.max(F.col("a.opened_date")).alias("latest_opened_date"))
)
