from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "right")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("s.store_name"),
        F.col("p.product_name"),
    )
    .orderBy(F.col("product_id").desc(), F.col("product_name").desc())
)
