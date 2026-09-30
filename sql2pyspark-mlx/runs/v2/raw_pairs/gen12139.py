from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("total_revenue") < 18000)
    .orderBy(F.col("first_sale_date").desc())
)
