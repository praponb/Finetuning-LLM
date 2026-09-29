from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(F.avg("revenue").alias("avg_revenue"))
    .filter(F.col("avg_revenue") > 2700)
    .orderBy("sale_month", "product_id", "sale_year")
    .limit(10)
)
