from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "product_name",
        "price",
        F.date_trunc("YEAR", "launch_date").alias("launch_date_period"),
    )
)
