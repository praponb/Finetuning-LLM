from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id").isin([10, 13, 19, 22]))
    .select(
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("i.store_id"),
    )
)
