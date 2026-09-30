from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "product_name",
        "category",
        F.month("launch_date").alias("launch_date_month"),
    )
    .orderBy("product_name")
)
