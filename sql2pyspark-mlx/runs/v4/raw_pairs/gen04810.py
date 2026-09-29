from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 9, 10]))
    .withColumn("sale_date_month", F.month("sale_date"))
)

agg = (
    recent_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.countDistinct("units").alias("unique_products"),
    )
)

result = agg.select("sale_quarter", "avg_revenue", "unique_products")
