from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(3200, 3800))
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.sum("units").alias("total_units"))
    .filter(F.col("total_units") > 220)
)
