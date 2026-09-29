from pyspark.sql import functions as F

store_totals = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 8]))
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.stddev_quantity") > 46)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("t.stddev_quantity"),
    )
    .orderBy("store_name", "id")
    .limit(50)
)
