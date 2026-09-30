from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.price") < 1030)
    .select(
        F.col("i.quantity"),
        F.col("s.city"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
    )
    .orderBy("city")
)
