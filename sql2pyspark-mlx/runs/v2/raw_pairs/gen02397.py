from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.month("sale_date").alias("sale_month"),
        F.when(F.col("units") >= 42, "high").when(F.col("units") >= 10, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg((F.max("units") - F.min("units")).alias("units_range"))
    .orderBy(F.col("sale_month").desc())
)
