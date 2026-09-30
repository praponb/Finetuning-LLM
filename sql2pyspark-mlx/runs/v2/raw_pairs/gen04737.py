from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3600)
    .agg(
        F.max("revenue").alias("max_revenue"),
        F.countDistinct("revenue").alias("unique_stores"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .orderBy(F.col("unique_stores").desc())
)
