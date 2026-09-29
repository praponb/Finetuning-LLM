from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.region").isNotNull())
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("price").desc())
)
