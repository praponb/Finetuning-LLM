from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "outer")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("p.brand"),
    )
)
