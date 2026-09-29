from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter((F.length(F.col("s.store_name")) >= 7) | F.col("i.product_id").isin([1, 5, 8, 9]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("p.launch_date"),
        F.col("p.product_name"),
    )
)
