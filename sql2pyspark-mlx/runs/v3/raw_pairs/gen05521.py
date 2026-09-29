from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("product_name").like("%Sho%"))

launch_month_summary = (
    valid_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    launch_month_summary
    .filter(F.col("stddev_cost") >= 51)
    .orderBy("first_launch_date", "launch_month")
    .limit(10)
)
