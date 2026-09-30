from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "store"]))
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .orderBy("stddev_discount")
)
