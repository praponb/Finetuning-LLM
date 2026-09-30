from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("p.id") == F.col("i.product_id"), "left")
    .select(
        F.col("p.cost"),
        F.col("p.price"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
    )
    .orderBy("cost")
)
