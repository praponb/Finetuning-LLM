from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-08-01")
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)
