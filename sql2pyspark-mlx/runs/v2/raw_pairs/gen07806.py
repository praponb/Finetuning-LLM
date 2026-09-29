from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "brand",
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
    )
    .orderBy(F.col("product_name_normalized").desc(), F.col("id").desc())
)
