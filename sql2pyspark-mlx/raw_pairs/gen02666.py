from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isNotNull() | (F.col("i.store_id") == 5))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("p.price"),
        (F.datediff(F.current_date(), F.col("i.last_restock")) / 30).alias("months_since_last_restock"),
    )
)
