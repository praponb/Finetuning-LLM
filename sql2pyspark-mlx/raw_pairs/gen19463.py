from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.count("units").alias("units_count"),
    )
    .orderBy("max_revenue", "product_id", "sale_month")
    .limit(3)
)
