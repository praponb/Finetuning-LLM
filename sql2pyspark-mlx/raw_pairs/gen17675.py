from pyspark.sql import functions as F

src = df_inventory.filter(F.col("store_id").isin([1, 2, 3]))

summary = (
    src
    .groupBy("store_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = summary
