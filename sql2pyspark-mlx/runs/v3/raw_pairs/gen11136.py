from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.cost") >= 260)
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.store_id"),
        F.col("s.sqft"),
        F.col("p.cost"),
        F.col("p.category"),
    )
    .orderBy("cost")
)
