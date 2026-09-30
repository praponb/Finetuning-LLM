from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(195, 344))
    .select(
        "id",
        "launch_date",
        F.concat_ws("-", "category", "product_name").alias("category_product_name_key"),
        F.concat_ws("-", "brand", "category").alias("brand_category_key"),
    )
)
