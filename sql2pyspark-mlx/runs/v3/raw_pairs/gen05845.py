from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        F.concat_ws("/", "category", "product_name").alias("category_product_name_key"),
    )
)
