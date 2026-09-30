from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "brand",
        "tags",
        F.weekofyear("launch_date").alias("launch_date_weekofyear"),
    )
)
