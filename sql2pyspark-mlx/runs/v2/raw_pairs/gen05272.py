from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select(
        "id",
        "product_name",
        "launch_date",
        "category",
        F.upper("brand").alias("brand_upper"),
    )
)
