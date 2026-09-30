from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id").isNotNull() | (F.col("p.price") > 420))
    .select(F.col("oi.unit_price"), F.col("p.category"), F.col("p.brand"))
    .orderBy("category")
)
