from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .select(
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("p.supplier_id"),
    )
    .orderBy("last_restock")
)
