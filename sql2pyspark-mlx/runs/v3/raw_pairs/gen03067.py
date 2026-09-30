from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 4000)
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
)
