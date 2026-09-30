from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter((F.col("s.opened_date") > "2024-03-01") | F.col("i.product_id").isin([3, 7]))
    .select(
        F.col("i.product_id"),
        F.col("p.launch_date"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.round((F.col("s.sqft") * 0.85), 2).alias("adjusted_sqft"),
    )
)
