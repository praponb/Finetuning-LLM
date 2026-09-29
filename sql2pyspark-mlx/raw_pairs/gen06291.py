from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") > 62)
    .select("supplier_id", "stddev_cost")
    .orderBy("stddev_cost")
)
