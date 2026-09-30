from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("last_restock") <= "2024-06-16")
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.min("quantity").alias("min_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.min_quantity") > 200)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("t.first_last_restock"),
        F.col("t.min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc(), "id")
    .limit(10)
)
