from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("*").alias("cnt"),
    )
)
