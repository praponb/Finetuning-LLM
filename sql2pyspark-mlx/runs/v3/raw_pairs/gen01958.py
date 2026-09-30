from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 3, 6]) & F.col("revenue").between(1100, 1600))
)

summary = (
    src
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.avg("units").alias("avg_units"))
)

result = (
    summary
    .filter(F.col("avg_units") < 15)
    .orderBy(F.col("avg_units").desc(), "sale_month")
    .limit(10)
)
