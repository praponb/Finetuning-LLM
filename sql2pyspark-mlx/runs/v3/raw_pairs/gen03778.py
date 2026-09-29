from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14400, "high").when(F.col("c.credit_limit") >= 4600, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.round(F.avg(F.col("o.amount")), 2).alias("avg_amount"))
    .orderBy(F.col("credit_limit_bucket").desc(), F.col("avg_amount").desc())
)
