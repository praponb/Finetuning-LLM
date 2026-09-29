from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.last_restock") > "2024-08-30")
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
        F.col("p.product_name"),
    )
    .orderBy("quantity")
)
