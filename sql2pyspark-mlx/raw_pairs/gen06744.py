from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.product_id").isin([1, 5, 10]))
    .select(
        F.col("p.cost"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("i.quantity"),
    )
)
