from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.quantity").isNotNull())
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("i.store_id"),
        F.col("i.product_id"),
    )
)
