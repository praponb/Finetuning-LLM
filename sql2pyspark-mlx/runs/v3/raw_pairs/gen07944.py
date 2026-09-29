from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .filter(F.col("i.product_id").isin([2, 6, 9, 10]))
    .select(
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("p.cost"),
        F.col("p.brand"),
    )
)
