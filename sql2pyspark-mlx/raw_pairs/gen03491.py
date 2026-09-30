from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
)
