from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("p.price"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
