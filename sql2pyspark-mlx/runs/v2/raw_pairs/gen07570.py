from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 8]))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
)

agg = (
    src
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = (
    agg
    .filter(F.col("total_revenue") >= 19000)
    .select("product_id", "total_revenue")
    .orderBy(F.col("total_revenue").desc())
)
