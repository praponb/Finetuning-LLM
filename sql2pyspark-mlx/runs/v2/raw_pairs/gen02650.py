from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .filter(F.col("oi.product_id").isNotNull())
    .join(df_products.filter(F.col("price") > 820).alias("p"), F.col("oi.product_id") == F.col("p.id"), "left_anti")
    .select(F.col("oi.id"), F.col("oi.order_id"), F.col("oi.unit_price"))
    .orderBy("unit_price")
)
