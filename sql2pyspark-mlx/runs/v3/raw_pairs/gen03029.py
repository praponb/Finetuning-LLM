from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.max("units").alias("max_units"),
    )
    .filter(F.col("max_units") >= 30)
    .select("sale_month", "total_revenue", "max_units")
    .orderBy(F.col("sale_month").desc())
)
