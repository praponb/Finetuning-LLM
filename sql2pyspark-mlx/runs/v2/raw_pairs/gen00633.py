from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4]))
    .groupBy("product_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.sum("units").alias("total_units"),
    )
    .filter(F.col("total_revenue") < 18000)
    .orderBy("product_id")
)
