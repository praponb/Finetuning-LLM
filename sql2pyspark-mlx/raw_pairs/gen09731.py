from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.year(F.col("orders.order_date")).alias("order_year"))
    .agg(
        F.round(F.avg(F.col("order_items.unit_price")), 2).alias("avg_unit_price"),
        F.count("*").alias("cnt"),
    )
    .orderBy("avg_unit_price")
)
