from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "stock_qty",
        F.substring("product_name", 1, 4).alias("product_name_prefix"),
    )
    .orderBy(F.col("product_name_prefix").desc())
)
