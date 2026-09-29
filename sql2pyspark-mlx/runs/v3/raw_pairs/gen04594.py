from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") < "2024-05-01") & (F.col("price") < 830))
    .select(
        "id",
        "brand",
        F.trim("product_name").alias("clean_product_name"),
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
    )
    .orderBy(F.col("brand").desc())
)
