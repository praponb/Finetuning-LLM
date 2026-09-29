from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .groupBy(
        "product_id",
        F.when(F.col("revenue") >= 3300, "high").when(F.col("revenue") >= 1700, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.countDistinct("units").alias("unique_products"),
        F.countDistinct("units").alias("unique_stores"),
    )
)
