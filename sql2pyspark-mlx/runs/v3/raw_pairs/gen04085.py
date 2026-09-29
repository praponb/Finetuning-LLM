from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2500, 3000) & (F.col("units") < 27))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc(), "total_units", "sale_month")
    .limit(3)
)
