from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.region"),
    )
    .orderBy(F.col("city").desc())
)
