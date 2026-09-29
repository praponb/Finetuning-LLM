from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.product_name").like("Wa%"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("s.region"),
        F.when(F.col("i.quantity") >= 231, "High").when(F.col("i.quantity") >= 91, "Medium").otherwise("Low").alias("quantity_level"),
    )
)
