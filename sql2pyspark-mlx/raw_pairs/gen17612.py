from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
        F.quarter("sale_date").alias("sale_quarter"),
    )
    .agg(
        F.count("units").alias("units_count"),
        F.min("sale_date").alias("min_sale_date"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("revenue_count") >= 1)
    .orderBy(F.col("revenue_count").desc())
)
