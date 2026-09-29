from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("o.amount") >= 1530)
    .groupBy(F.col("o.status"))
    .agg(
        F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"),
        F.sum(F.col("oi.unit_price")).alias("total_unit_price"),
    )
    .orderBy("total_unit_price")
)
