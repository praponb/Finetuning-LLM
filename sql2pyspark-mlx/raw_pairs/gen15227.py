from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 9)
    .groupBy("product_id", "store_id")
    .agg(
        F.countDistinct("units").alias("unique_stores"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
    )
    .orderBy(F.col("revenue_range").desc(), "product_id", "store_id")
    .limit(20)
)
