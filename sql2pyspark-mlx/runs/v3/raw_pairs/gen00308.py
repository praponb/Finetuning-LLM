from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 20) | F.col("store_id").isin([2, 3, 4, 5]))
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)
