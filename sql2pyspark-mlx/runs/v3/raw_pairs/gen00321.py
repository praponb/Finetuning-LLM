from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"), "store_id")
    .agg(F.avg("units").alias("avg_units"))
)
