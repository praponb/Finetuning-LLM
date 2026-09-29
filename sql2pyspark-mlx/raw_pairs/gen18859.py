from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(
        F.when(F.col("c.age") >= 57, "high").when(F.col("c.age") >= 40, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"))
    .filter(F.col("avg_shipping_fee") > 12)
    .orderBy("age_bucket")
)
