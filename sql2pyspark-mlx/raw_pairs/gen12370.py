from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("min_revenue") > 1700)
    .select("sale_month", "min_revenue", "min_units")
)
