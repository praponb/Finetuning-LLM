from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .groupBy("brand", F.month("launch_date").alias("launch_month"))
    .agg(
        F.countDistinct("stock_qty").alias("unique_categorys"),
        F.min("launch_date").alias("min_launch_date"),
    )
    .filter(F.col("unique_categorys") < 9)
)
