from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.alias("i"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.quantity") <= 189)
    .select(
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("i.store_id"),
    )
    .orderBy("product_id")
)
