from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 31)
    .cube("product_id", "store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.col("unique_stores").desc())
)
