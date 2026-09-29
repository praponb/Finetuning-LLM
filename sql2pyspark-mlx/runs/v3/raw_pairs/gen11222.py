from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter((F.col("i.quantity") >= 110) & (F.col("i.quantity") < 108))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("p.brand"),
        F.col("s.city"),
    )
)
