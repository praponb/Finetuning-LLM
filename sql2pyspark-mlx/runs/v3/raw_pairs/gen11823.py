from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .orderBy("last_sale_date", "sale_month")
    .limit(10)
)
