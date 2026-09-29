from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") > 1100) & (F.col("launch_date") <= "2023-03-27") & F.array_contains("tags", "clearance"))
    .select(
        "id",
        "brand",
        "stock_qty",
        F.upper("product_name").alias("product_name_upper"),
    )
    .orderBy("product_name_upper", "id")
)
