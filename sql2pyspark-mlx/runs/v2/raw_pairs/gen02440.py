from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 8]))
    .withColumn("sale_date_month", F.month("sale_date"))
)

agg = (
    src
    .groupBy("store_id")
    .agg(F.max("revenue").alias("max_revenue"))
)

result = agg.filter(F.col("max_revenue") > 2600)
