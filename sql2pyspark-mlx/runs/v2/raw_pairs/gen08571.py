from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.price") >= 720)
    .groupBy(F.col("s.country"))
    .agg(F.round(F.stddev(F.col("oi.quantity")), 2).alias("stddev_quantity"))
    .orderBy("stddev_quantity", "country")
    .limit(50)
)
