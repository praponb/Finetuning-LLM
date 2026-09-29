from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3900)
    .groupBy(F.year("sale_date").alias("sale_year"), "product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .orderBy(F.col("total_revenue").desc())
)
