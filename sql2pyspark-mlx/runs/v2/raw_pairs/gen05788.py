from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.min("sale_date").alias("first_sale_date"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy("total_revenue")
)
