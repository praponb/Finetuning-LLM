from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("s.opened_date"),
        F.col("p.brand"),
        F.year(F.col("s.opened_date")).alias("opened_date_year"),
    )
)
