from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 520)
    .select(
        "id",
        "stock_qty",
        F.concat("category", F.lit(", "), "product_name").alias("category_product_name"),
    )
)
