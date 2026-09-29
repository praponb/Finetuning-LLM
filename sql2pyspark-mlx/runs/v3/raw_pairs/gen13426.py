from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "category",
        "product_name",
        F.substring("brand", 1, 3).alias("brand_prefix"),
        F.sort_array("tags").alias("sorted_tags"),
    )
)
