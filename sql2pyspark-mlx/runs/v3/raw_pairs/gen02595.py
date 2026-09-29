from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("units") >= 42, "high").when(F.col("units") >= 8, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .orderBy("units_bucket")
)
