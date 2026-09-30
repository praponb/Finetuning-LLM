from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1400)
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
    .orderBy("stddev_units", "sale_quarter")
    .limit(1)
)
