from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.month("sale_date").isin([1, 7, 8]))

summary = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.max("units").alias("max_units"),
    )
)

result = summary.orderBy(F.col("product_id").desc(), "max_units")
