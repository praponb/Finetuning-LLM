from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([7, 9]))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.sum("units").alias("total_units"))
    .filter(F.col("total_units") >= 140)
    .select("sale_month", "total_units")
    .orderBy("sale_month")
    .limit(1)
)
