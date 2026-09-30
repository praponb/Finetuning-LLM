from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)
