from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5]))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "product_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
    .orderBy("sale_month")
)
