from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.year("opened_date") == 2025).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("i.product_id") == 1)
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("i.quantity"),
    )
)
