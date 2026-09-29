from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") != "Initech")
    .select(
        "id",
        "stock_qty",
        "product_name",
        F.concat("brand", F.lit(" "), "category").alias("brand_category"),
        F.col("price").cast("int").alias("price_int"),
    )
)
