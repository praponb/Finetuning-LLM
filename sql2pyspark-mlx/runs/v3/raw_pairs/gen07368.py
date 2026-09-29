from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "category",
        "price",
        "product_name",
        F.dayofmonth("launch_date").alias("launch_date_day"),
    )
    .orderBy("product_name")
)
