from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 5, 9]) | (F.col("quantity") < 144))
)

summary = (
    base
    .groupBy(F.year("last_restock").alias("last_restock_year"), "product_id")
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    summary
    .filter(F.col("stddev_quantity") < 39)
    .select("last_restock_year", "product_id", "stddev_quantity")
    .orderBy(F.col("last_restock_year").desc(), "product_id")
    .limit(3)
)
