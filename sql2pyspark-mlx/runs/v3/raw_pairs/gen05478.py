from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.unique_products") >= 4)
    .select(
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("ss.unique_products"),
        F.col("ss.quantity_count"),
    )
    .orderBy(F.col("region").desc())
)
