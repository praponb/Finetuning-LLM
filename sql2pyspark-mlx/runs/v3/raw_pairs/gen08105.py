from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("i.quantity") == 193)
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("s.store_name"),
    )
)
