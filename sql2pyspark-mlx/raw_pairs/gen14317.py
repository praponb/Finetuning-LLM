from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .select(
        F.col("s.opened_date"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
    )
)
