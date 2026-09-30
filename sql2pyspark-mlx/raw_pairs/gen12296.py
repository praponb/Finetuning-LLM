from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.signup_date").between("2023-12-01", "2024-02-06"))
    .groupBy(
        F.col("o.channel"),
        F.when(F.col("o.shipping_fee") >= 31, "high").when(F.col("o.shipping_fee") >= 11, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
        F.sum(F.col("p.amount")).alias("total_amount"),
    )
    .filter(F.col("avg_amount") > 24)
)
