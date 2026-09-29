from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id").isin([1, 2, 3, 8]))

sale_month_summary = (
    base
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    sale_month_summary
    .orderBy(F.col("max_revenue").desc(), F.col("sale_month").desc())
    .limit(20)
)
