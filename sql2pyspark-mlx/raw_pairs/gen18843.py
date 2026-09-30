from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter((F.col("stock_qty") < 332) & (F.col("stock_qty") != 185))
    .withColumn("product_name_prefix", F.substring("product_name", 1, 2))
)

summary = (
    valid_products
    .groupBy("category")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = (
    summary
    .select("category", "min_launch_date")
    .orderBy("category", "min_launch_date")
)
