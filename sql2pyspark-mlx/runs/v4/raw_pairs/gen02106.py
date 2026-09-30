from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("units") <= 35)

agg = (
    filtered_store_sales
    .groupBy(
        F.when(F.col("units") >= 32, "high").when(F.col("units") >= 13, "mid").otherwise("low").alias("units_bucket"),
        "product_id",
    )
    .agg(F.avg("units").alias("avg_units"))
)

result = (
    agg
    .filter(F.col("avg_units") >= 38)
    .orderBy(F.col("avg_units").desc())
)
