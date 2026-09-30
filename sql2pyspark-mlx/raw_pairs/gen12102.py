from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([2, 4, 5]))
    .groupBy(
        F.when(F.col("units") >= 33, "high").when(F.col("units") >= 16, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.count("*").alias("store_sale_count"),
        F.count("*").alias("row_count"),
    )
    .orderBy("store_sale_count", "units_bucket")
    .limit(3)
)
