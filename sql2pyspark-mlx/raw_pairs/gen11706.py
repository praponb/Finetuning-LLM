from pyspark.sql import functions as F

recent_products = (
    df_products
    .filter(F.col("stock_qty").between(200, 283) & (F.col("stock_qty") != 193))
    .withColumn("stock_qty_filled", F.coalesce("stock_qty", F.lit(0)))
)

agg = (
    recent_products
    .groupBy("category")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    agg
    .filter(F.col("stddev_cost") > 27)
    .orderBy(F.col("category").desc())
    .limit(5)
)
