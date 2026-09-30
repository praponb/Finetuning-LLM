from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.last_restock"),
        F.col("p.cost"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.year(F.col("i.last_restock")).alias("last_restock_year"),
    )
)
