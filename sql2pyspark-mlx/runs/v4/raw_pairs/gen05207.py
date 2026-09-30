from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.cost") < 200)
    .groupBy(F.col("p.brand"), F.col("p.supplier_id"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc())
)
