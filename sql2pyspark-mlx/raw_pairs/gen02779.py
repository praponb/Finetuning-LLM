from pyspark.sql import functions as F

valid_products = df_products.filter(F.col("product_name").like("%Laptop%"))

summary = (
    valid_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    summary
    .select("launch_month", "fashion_count")
    .orderBy(F.col("fashion_count").desc())
)
