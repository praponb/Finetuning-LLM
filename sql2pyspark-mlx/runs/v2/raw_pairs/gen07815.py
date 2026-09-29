from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.countDistinct("revenue").alias("unique_products"))
    .orderBy("sale_quarter")
)
