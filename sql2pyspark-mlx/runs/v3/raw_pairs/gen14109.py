from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("product_id").isin([2, 7, 9]))

summary = (
    filtered_store_sales
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 39, "high").when(F.col("units") >= 11, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.max("units").alias("max_units"),
        F.avg("units").alias("avg_units"),
    )
)

result = (
    summary
    .filter(F.col("avg_units") >= 33)
    .orderBy(F.col("product_id").desc())
)
