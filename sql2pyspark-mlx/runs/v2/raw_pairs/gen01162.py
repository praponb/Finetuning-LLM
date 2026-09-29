from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("avg_units") > 37)
    .orderBy(F.col("product_id").desc(), "total_count", "store_id")
    .limit(5)
)
