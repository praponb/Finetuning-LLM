from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") > 46)
    .select("product_id", "stddev_quantity")
    .orderBy(F.col("product_id").desc(), F.col("stddev_quantity").desc())
    .limit(5)
)
