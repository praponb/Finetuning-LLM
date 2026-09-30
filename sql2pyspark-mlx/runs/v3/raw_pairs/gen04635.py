from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter((F.col("i.store_id") == 2) | (F.col("s.sqft") != 15481))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("s.store_name"),
        F.ceil(F.col("i.quantity")).alias("quantity_whole"),
    )
    .orderBy(F.col("quantity").desc())
)
