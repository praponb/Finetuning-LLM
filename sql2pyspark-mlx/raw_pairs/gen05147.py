from pyspark.sql import functions as F

kept = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 6])), "left_anti")
)

result = (
    kept
    .groupBy(F.when(F.col("sqft") >= 14888, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.size(F.collect_set("region")).alias("num_regions"),
    )
)
