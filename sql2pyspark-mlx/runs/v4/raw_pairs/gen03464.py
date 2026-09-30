from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([1, 10, 12]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("i.quantity").cast("double").alias("quantity_num"),
    )
)
