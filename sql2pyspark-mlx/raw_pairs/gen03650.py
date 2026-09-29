from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isNull() & F.month("launch_date").isin([3, 7, 10]))
    .select(
        "id",
        "category",
        "stock_qty",
        F.when(F.col("brand") == "Initech", 1).otherwise(0).alias("is_initech"),
        F.concat_ws("-", "category", "brand").alias("category_brand_key"),
    )
)
