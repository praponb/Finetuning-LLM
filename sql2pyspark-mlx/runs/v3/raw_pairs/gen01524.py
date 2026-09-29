from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(~((F.col("units") <= 16) & ((F.col("units") <= 39) | F.month("sale_date").isin([3, 5, 11]))))
)

agg = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    agg
    .filter(F.col("total_revenue") < 18000)
    .orderBy("product_id")
    .limit(1)
)
