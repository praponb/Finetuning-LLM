from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.max("units").alias("max_units"))
    .filter(F.col("max_units") < 30)
    .orderBy("max_units", "sale_month")
    .limit(1)
)
