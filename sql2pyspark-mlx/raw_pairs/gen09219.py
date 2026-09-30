from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 15600, "high").when(F.col("c.credit_limit") >= 6000, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.max(F.col("a.opened_date")).alias("latest_opened_date"),
        F.min(F.col("a.opened_date")).alias("first_opened_date"),
    )
    .orderBy("latest_opened_date", "credit_limit_bucket")
    .limit(20)
)
