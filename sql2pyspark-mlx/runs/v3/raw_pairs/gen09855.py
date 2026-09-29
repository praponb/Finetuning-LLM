from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("supplier_id") == 16) | (F.col("cost") > 550))
    .withColumn("clean_category", F.trim("category"))
)

agg = (
    base
    .groupBy("supplier_id")
    .agg(F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"))
)

result = (
    agg
    .filter(F.col("avg_stock_qty") < 24)
    .select("supplier_id", "avg_stock_qty")
    .orderBy(F.col("avg_stock_qty").desc(), F.col("supplier_id").desc())
    .limit(3)
)
