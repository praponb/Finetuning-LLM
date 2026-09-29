from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("i.product_id").isin([5, 6, 8]))
    .select(
        F.col("p.category"),
        F.col("p.cost"),
        F.col("p.price"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("i.product_id"),
    )
)
