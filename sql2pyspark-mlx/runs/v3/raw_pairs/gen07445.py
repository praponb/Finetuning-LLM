from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Chair%"))
    .groupBy(
        F.when(F.col("price") >= 910, "high").when(F.col("price") >= 250, "mid").otherwise("low").alias("price_bucket"),
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
    )
    .agg(F.count("*").alias("product_count"))
    .filter(F.col("product_count") < 8)
)
