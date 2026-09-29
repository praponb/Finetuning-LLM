from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 19) & (F.col("units") == 14) & (F.col("sale_date") < "2023-04-25"))
    .groupBy("store_id", "product_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)
