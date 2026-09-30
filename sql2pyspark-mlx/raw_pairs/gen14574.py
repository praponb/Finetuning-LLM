from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-08-06").cast("date"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "tags",
        F.length("category").alias("category_length"),
    )
)

result = (
    filtered
    .select("brand", "id")
    .subtract(
        df_products
        .filter(F.col("launch_date") <= "2024-09-30")
        .select("brand", "id")
    )
)
