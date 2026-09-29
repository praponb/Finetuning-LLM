from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("units") < 39) & (F.col("units") >= 25)) | F.col("product_id").isin([1, 3]))
    .groupBy("product_id")
    .agg(F.round(F.avg("revenue"), 2).alias("avg_revenue"))
)
