from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.city") == "Berlin") | F.col("s.store_name").isNull())
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("s.sqft"),
    )
)
