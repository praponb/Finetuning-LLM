from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.sum("units").alias("total_units"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("total_units") >= 140)
)
