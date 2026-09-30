from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 3800) & (F.col("sale_date") <= "2024-09-01") & (F.col("sale_date") <= "2023-10-27"))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.countDistinct("units").alias("unique_products"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") < 48)
    .orderBy(F.col("sale_month").desc())
)
