from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.product_name").isNotNull())
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
)
