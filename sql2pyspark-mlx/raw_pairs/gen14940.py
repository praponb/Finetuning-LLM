from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4]))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.count("revenue").alias("revenue_count"),
        F.avg("units").alias("avg_units"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("min_units") > 20)
    .drop("min_units")
)
