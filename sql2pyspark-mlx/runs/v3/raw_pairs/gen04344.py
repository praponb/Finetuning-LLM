from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("i.store_id") == 6)
    .select(
        F.col("i.product_id"),
        F.col("i.quantity"),
        F.col("s.region"),
        F.add_months(F.col("i.last_restock"), 6).alias("last_restock_plus_months"),
    )
)
