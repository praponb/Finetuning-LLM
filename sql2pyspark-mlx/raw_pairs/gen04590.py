from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("p.supplier_id"), F.col("p.category"))
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .filter(F.col("quantity_count") >= 2)
    .orderBy("category", "quantity_count")
)
