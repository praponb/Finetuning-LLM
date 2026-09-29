from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock") > "2023-06-01")
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
    )
)
