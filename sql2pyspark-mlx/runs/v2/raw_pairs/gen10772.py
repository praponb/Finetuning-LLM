from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.min("sale_date").alias("min_sale_date"),
    )
)
