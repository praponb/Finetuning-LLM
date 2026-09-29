from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") > "2023-07-01")
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
)
