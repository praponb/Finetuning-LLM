from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("total_revenue") < 20000)
    .select("product_id", "total_revenue", "units_count")
    .orderBy(F.col("product_id").desc())
)
