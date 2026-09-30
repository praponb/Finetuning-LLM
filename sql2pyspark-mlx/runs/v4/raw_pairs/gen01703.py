from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
    .filter(F.col("total_units") > 100)
)
