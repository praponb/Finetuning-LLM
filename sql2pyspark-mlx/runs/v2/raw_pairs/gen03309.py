from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "brand",
        "cost",
        F.concat_ws("-", "category", "brand").alias("category_brand_key"),
    )
)
