from pyspark.sql import functions as F

base = df_inventory.filter(F.month("last_restock").isin([3, 8, 12]))

summary = (
    base
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    summary
    .filter(F.col("stddev_quantity") < 41)
    .select("store_id", "stddev_quantity")
    .orderBy(F.col("stddev_quantity").desc())
)
