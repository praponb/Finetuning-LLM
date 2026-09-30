from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("units").between(21, 30) & (F.col("units") < 22) & (F.col("revenue") > 2200))
    .withColumn("revenue_int", F.col("revenue").cast("int"))
)

store_sales_stats = (
    src
    .groupBy("store_id")
    .agg(F.countDistinct("revenue_int").alias("unique_products"))
)

result = (
    store_sales_stats
    .select("store_id", "unique_products")
    .orderBy(F.col("unique_products").desc())
)
