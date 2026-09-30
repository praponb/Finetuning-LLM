from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 10)
    .groupBy("product_id", "store_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.max("revenue").alias("max_revenue"),
    )
    .filter(F.col("max_revenue") > 2700)
    .drop("max_revenue")
    .orderBy("first_sale_date", "product_id")
)
