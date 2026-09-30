from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("p.launch_date"),
        F.col("s.region"),
    )
    .orderBy("last_restock")
)
