from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("units").between(25, 34))

revenue_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("revenue") >= 4200, "high").when(F.col("revenue") >= 1700, "mid").otherwise("low").alias("revenue_bucket"),
        "store_id",
    )
    .agg(F.min("sale_date").alias("first_sale_date"))
)

result = revenue_bucket_summary
