from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("p.method") == "card")
    .groupBy(
        F.when(F.col("c.age") >= 72, "high").when(F.col("c.age") >= 37, "mid").otherwise("low").alias("age_bucket"),
        F.col("c.country"),
    )
    .agg(F.min(F.col("p.amount")).alias("min_amount"))
    .filter(F.col("min_amount") >= 740)
    .orderBy("age_bucket")
)
