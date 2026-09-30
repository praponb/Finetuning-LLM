from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.quarter("sale_date").alias("sale_quarter"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.sum("units").alias("total_units"),
    )
    .filter(F.col("avg_revenue") > 3300)
)
