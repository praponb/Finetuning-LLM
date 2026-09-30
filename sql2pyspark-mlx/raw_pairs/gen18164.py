from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy(F.col("city").desc())
)
