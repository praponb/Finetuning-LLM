from pyspark.sql import functions as F

inventory_by_store = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(inventory_by_store.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.row_count") >= 8)
    .select(
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("t.row_count"),
        F.col("t.total_quantity"),
    )
    .orderBy(F.col("opened_date").desc())
)
