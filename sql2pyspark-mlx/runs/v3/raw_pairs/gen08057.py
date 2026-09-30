from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2700)
    .groupBy("store_id", "product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.countDistinct("units").alias("unique_products"),
    )
    .orderBy(F.col("product_id").desc())
)
