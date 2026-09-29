from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 34, "high").when(F.col("units") >= 10, "mid").otherwise("low").alias("units_bucket"),
        "product_id",
    )
    .agg(F.countDistinct("revenue").alias("unique_products"))
    .orderBy(F.col("units_bucket").desc())
)
