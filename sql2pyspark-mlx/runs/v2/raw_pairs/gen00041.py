from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)
