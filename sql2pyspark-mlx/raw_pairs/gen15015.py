from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.opened_date"),
        F.date_trunc("month", F.col("i.last_restock")).alias("last_restock_period"),
    )
)
