from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") <= 58)
    .groupBy(F.col("c.country"), F.col("c.segment"))
    .agg(
        F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .orderBy("stddev_unit_price", "segment", "country")
    .limit(10)
)
