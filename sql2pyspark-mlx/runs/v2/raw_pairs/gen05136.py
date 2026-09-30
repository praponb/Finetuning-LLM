from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 27)
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.max("units").alias("max_units"),
    )
    .orderBy("sale_month", "min_revenue")
)
