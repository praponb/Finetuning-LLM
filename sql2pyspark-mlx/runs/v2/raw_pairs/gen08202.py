from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 2100)
    .agg(
        F.count("*").alias("row_count"),
        F.min("sale_date").alias("min_sale_date"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
)
