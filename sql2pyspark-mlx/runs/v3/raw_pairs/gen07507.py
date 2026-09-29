from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 3700)
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.max("sale_date").alias("latest_sale_date"),
    )
    .filter(F.col("max_revenue") > 2600)
    .orderBy("max_revenue", "product_id")
    .limit(20)
)
