from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter(F.month("launch_date").isin([1, 11, 12]) & F.col("stock_qty").between(219, 332) & (F.col("price") >= 1120))
    .withColumn("brand_length", F.length("brand"))
)

summary = (
    valid_products
    .groupBy(F.month("launch_date").alias("launch_month"))
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.max("launch_date").alias("last_launch_date"),
    )
)

result = (
    summary
    .filter(F.col("unique_brands") >= 7)
    .select("launch_month", "last_launch_date")
    .orderBy("last_launch_date", F.col("launch_month").desc())
)
