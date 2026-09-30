from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 1400)
    .groupBy(
        "product_id",
        F.when(F.col("revenue") >= 3700, "high").when(F.col("revenue") >= 600, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.countDistinct("revenue").alias("unique_stores"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
    .orderBy(F.col("unique_stores").desc())
)
