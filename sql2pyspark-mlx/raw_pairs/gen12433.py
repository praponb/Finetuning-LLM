from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("product_name").like("Y%") | (F.col("brand") != "Initech"))

summary = (
    filtered_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.min("launch_date").alias("first_launch_date"))
)

result = (
    summary
    .select("launch_month", "first_launch_date")
    .orderBy("first_launch_date")
)
