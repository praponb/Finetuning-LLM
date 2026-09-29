from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("i.product_id").isin([3, 10]))
    .groupBy(F.col("p.category"))
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .orderBy("category")
)
