from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter((F.col("p.price") > 760) | (F.upper(F.trim(F.col("s.store_name"))) == "DOWNTOWN EXPRESS"))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("p.cost"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
    .orderBy("store_name", F.col("region").desc())
)
