from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.col("revenue") >= 3600, "high").when(F.col("revenue") >= 1700, "mid").otherwise("low").alias("revenue_bucket"),
    )
    .agg(F.min("sale_date").alias("first_sale_date"))
)
