from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("store_id").isin([1, 2, 3, 4]))

sale_month_summary = (
    base
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("units").alias("min_units"),
    )
)

result = (
    sale_month_summary
    .filter(F.col("avg_units") > 49)
    .select("sale_month", "avg_units", "min_units")
    .orderBy("sale_month")
)
