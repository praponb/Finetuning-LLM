from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        F.concat_ws("-", "brand", "product_name").alias("brand_product_name_key"),
    )
    .orderBy(F.col("brand_product_name_key").desc())
)
