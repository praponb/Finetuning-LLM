from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("p.brand").isNotNull())
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.product_name"),
        F.col("p.brand"),
        F.col("s.city"),
    )
)
