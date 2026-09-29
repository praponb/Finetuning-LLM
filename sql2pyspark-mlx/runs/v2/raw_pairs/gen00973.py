from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("p.stock_qty").between(152, 234))
    .groupBy(F.col("p.brand"))
    .agg(
        F.count(F.col("oi.quantity")).alias("quantity_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc(), "brand")
    .limit(5)
)
