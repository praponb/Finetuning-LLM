from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id").isin([1, 9]))
    .select(
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("p.cost"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
    )
    .orderBy(F.col("product_id").desc())
)
