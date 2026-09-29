from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.supplier_name").isNotNull())
    .groupBy(F.col("s.country"))
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .orderBy("country")
)
