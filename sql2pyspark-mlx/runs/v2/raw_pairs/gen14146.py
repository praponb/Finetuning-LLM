from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-05-01")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.countDistinct("units").alias("unique_products"),
        F.min("revenue").alias("min_revenue"),
    )
)
