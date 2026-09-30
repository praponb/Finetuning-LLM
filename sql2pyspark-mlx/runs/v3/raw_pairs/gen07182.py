from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.sum("units").alias("total_units"))
    .filter(F.col("total_units") > 200)
    .orderBy("sale_month")
)
