from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(F.col("i.store_id").isin([2, 3, 6]) & F.col("i.product_id").isin([6, 10]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.coalesce(F.col("s.sqft"), F.lit(0)).alias("sqft_filled"),
    )
    .orderBy("product_id")
)
