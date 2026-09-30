from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(~(F.col("p.supplier_id").isNotNull() & ~F.col("s.store_name").like("%Riverside%")))
    .select(F.col("i.id"), F.col("i.store_id"), F.col("p.price"), F.col("s.sqft"))
    .orderBy(F.col("id").desc())
)
