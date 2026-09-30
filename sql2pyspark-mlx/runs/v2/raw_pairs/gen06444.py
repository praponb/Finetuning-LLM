from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("i.last_restock") < "2025-09-07")
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("store_name").desc(), "opened_date")
)
