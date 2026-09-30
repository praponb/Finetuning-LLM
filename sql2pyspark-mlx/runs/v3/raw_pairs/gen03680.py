from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("price") <= 580))
    .groupBy("category", "brand")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .orderBy(F.col("category").desc(), "brand")
    .limit(5)
)
