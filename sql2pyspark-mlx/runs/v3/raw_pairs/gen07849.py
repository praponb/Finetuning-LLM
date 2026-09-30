from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") >= 155)
    .groupBy(
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        F.when(F.col("cost") >= 640, "high").when(F.col("cost") >= 100, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.countDistinct("cost").alias("unique_brands"))
    .filter(F.col("unique_brands") > 2)
    .orderBy(F.col("unique_brands").desc(), "cost_bucket")
)
