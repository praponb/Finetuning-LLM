from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "product_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
    .orderBy("sale_month", "product_id")
)
