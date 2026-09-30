from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .filter(F.col("i.product_id").isin([2, 7]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("p.price"),
        F.col("p.supplier_id"),
    )
)
