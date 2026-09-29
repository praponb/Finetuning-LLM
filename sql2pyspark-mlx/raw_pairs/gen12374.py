from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-06-01", "2024-04-03"))
    .groupBy("product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("avg_revenue") > 1800)
)
