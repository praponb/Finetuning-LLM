from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "right")
    .filter(F.col("s.city").isin(["Chiang Mai", "London"]) & F.col("s.store_name").like("%Town%"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("p.cost"),
        F.col("s.city"),
        F.substring(F.col("s.city"), 1, 2).alias("city_prefix"),
    )
    .orderBy("cost")
)
