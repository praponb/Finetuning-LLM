from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "product_name",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
    )
)
