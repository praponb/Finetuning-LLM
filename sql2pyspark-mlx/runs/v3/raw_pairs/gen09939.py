from pyspark.sql import functions as F

base = df_inventory.filter(F.col("store_id").isin([1, 2]))

store_id_summary = (
    base
    .groupBy("store_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    store_id_summary
    .filter(F.col("quantity_count") > 6)
    .select("store_id", "quantity_count", "min_quantity")
)
