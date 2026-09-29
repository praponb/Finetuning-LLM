from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("units") < 33)

store_sales_stats = (
    src
    .groupBy("store_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    store_sales_stats
    .select("store_id", "latest_sale_date")
    .orderBy("store_id")
)
