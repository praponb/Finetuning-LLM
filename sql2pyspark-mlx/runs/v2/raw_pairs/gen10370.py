from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("sqft").between(11677, 15352)).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("i.quantity"),
    )
)
