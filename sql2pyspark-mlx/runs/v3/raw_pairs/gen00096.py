from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 3, 10]))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"), "store_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("revenue").alias("revenue_count"),
    )
)
