from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("supplier_id").isin([7, 18, 36]))
    .withColumn("category_lower", F.lower("category"))
)

result = (
    valid
    .groupBy("supplier_id")
    .pivot("category", ["Fashion", "Books"])
    .agg(F.avg("stock_qty"))
)
