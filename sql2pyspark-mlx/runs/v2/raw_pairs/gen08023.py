from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 4) & (F.col("units") >= 24))
    .agg(F.min("sale_date").alias("min_sale_date"))
    .orderBy("min_sale_date")
)
