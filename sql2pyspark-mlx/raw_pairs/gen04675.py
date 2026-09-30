from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(((F.col("revenue") < 3600) & (F.col("revenue") >= 3700)) | F.col("product_id").isin([2, 3, 7, 9]))
)

agg = (
    base
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = agg
