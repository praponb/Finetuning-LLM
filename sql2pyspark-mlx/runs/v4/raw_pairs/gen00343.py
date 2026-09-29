from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id").isin([4, 5]))

product_id_summary = (
    base
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = product_id_summary.orderBy(F.col("product_id").desc())
