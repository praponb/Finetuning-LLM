from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("i.quantity") != 217)
    .groupBy(F.col("p.brand"), F.col("p.supplier_id"))
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .orderBy("supplier_id", "brand")
    .limit(5)
)
