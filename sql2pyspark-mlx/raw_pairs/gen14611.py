from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "tags",
        F.regexp_replace("product_name", " ", "_").alias("product_name_slug"),
    )
    .orderBy("id")
)
