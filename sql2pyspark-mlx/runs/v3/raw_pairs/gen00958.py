from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.opened_date") < "2024-09-08")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 13100, "high").when(F.col("c.credit_limit") >= 5200, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.min(F.col("a.opened_date")).alias("first_opened_date"))
    .orderBy(F.col("credit_limit_bucket").desc(), "first_opened_date")
)
