from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.store_id").isin([1, 2, 6]) & (F.col("i.quantity") != 73))
    .select(
        F.col("p.launch_date"),
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.col("i.quantity"),
    )
    .orderBy(F.col("quantity").desc())
)
