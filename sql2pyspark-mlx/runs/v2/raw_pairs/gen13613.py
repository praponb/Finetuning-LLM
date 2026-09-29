from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.product_id") == 9)
    .select(
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("p.cost"),
        F.col("s.opened_date"),
    )
)
