from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .select(
        F.col("i.last_restock"),
        F.col("p.category"),
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.when(F.col("p.price") >= 1250, "High").when(F.col("p.price") >= 440, "Medium").otherwise("Low").alias("price_level"),
    )
)
