from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") > 10)
    .select(
        "tags",
        "cost",
        F.year("launch_date").alias("launch_date_year"),
        F.length("brand").alias("brand_length"),
    )
)
