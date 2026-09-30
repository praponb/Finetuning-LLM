from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(
        F.count("*").alias("total_count"),
        F.count("revenue").alias("revenue_count"),
    )
    .orderBy("total_count")
)
