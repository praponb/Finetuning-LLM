from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter((F.size(F.col("p.tags")) > 2) | F.lower(F.col("s.store_name")).like("%all%"))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
    .orderBy(F.col("cost").desc())
)
