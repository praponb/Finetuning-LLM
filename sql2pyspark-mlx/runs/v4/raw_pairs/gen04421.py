from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNull())
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)
