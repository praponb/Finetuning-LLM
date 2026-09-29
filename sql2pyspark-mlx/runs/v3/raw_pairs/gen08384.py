from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 39, "high").when(F.col("units") >= 6, "mid").otherwise("low").alias("units_bucket"),
        "store_id",
    )
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .filter(F.col("revenue_range") > 78)
)
