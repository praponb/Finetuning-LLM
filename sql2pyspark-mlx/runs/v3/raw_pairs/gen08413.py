from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "product_name",
        "launch_date",
        F.substring("product_name", 1, 2).alias("product_name_prefix"),
    )
)
