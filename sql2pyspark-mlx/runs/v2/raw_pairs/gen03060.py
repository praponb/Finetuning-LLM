from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Home", "Fashion", "Sports"]) & (F.col("stock_qty") <= 280) & F.array_contains("tags", "eco"))
    .groupBy("category")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
    .orderBy("stddev_price")
)
