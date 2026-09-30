from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") > 6)
    .groupBy("category")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.max("launch_date").alias("latest_launch_date"),
    )
    .filter(F.col("price_range") >= 80)
    .select("category", "price_range", "latest_launch_date")
)
