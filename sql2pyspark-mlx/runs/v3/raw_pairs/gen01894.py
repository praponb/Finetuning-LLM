from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.lower(F.col("c.email")).like("%any%"))
    .groupBy(
        F.when(F.col("c.age") >= 67, "high").when(F.col("c.age") >= 26, "mid").otherwise("low").alias("age_bucket"),
        F.col("c.is_active"),
    )
    .agg(F.countDistinct(F.col("a.balance")).alias("unique_statuss"))
    .orderBy("unique_statuss")
)
