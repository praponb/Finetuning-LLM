from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 9, 10]))
    .groupBy(
        F.when(F.col("units") >= 43, "high").when(F.col("units") >= 9, "mid").otherwise("low").alias("units_bucket"),
        "product_id",
    )
    .agg(F.min("sale_date").alias("min_sale_date"))
)
