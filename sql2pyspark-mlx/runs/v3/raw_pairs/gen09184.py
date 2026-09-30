from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-02-01", "2025-03-01") & F.col("store_id").isin([2, 5]))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.max("revenue").alias("max_revenue"),
        F.sum("revenue").alias("total_revenue"),
    )
)
