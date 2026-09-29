from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 7]))
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_stores.alias("stores")
    .join(store_totals.alias("store_totals"), F.col("stores.id") == F.col("store_totals.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.city"),
        F.col("store_totals.max_quantity"),
    )
)
