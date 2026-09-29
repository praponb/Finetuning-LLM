from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .groupBy(F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"))
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .filter(F.col("quantity_count") >= 4)
    .orderBy(F.col("quantity_count").desc(), "order_month")
    .limit(1)
)
