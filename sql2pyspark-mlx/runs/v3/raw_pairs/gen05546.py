from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("product_id").isin([3, 10]) & (F.col("units") < 35))

product_id_summary = (
    filtered_store_sales
    .groupBy(
        "product_id",
        F.when(F.col("revenue") >= 3200, "high").when(F.col("revenue") >= 800, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
)

result = (
    product_id_summary
    .filter(F.col("avg_units") >= 35)
    .orderBy(F.col("product_id").desc(), "revenue_bucket")
    .limit(5)
)
