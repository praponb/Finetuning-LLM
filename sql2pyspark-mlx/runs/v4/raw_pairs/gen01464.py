from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.city") == "Chiang Mai")
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sum(F.col("o.shipping_fee")).alias("total_shipping_fee"),
    )
    .filter(F.col("total_shipping_fee") >= 160)
)
