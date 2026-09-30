from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("s.city").isin(["Berlin", "Bangkok"]))
    .select(
        F.col("i.store_id"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("p.category"),
        F.col("p.brand"),
    )
)
