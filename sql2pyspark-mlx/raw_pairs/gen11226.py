from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .select(
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("launch_date").desc(), F.col("product_id").desc())
)
