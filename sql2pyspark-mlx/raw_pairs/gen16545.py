from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.min("revenue").alias("min_revenue"),
    )
    .orderBy(F.col("min_revenue").desc())
)
