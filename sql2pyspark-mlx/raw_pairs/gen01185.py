from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"),
    )
    .orderBy("city", "avg_shipping_fee")
)
