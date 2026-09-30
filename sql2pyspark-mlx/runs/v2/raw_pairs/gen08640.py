from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("units").alias("min_units"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("min_units") >= 30)
    .select("product_id", "min_units", "revenue_count")
)
