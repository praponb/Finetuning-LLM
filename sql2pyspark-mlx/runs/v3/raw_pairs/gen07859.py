from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Noi%"))
    .select(
        "product_name",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
    )
)
