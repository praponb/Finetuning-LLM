from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") != "Sports") & (F.col("price").between(730, 1200) | (F.size("tags") >= 2)))
    .select(
        "id",
        "product_name",
        "tags",
        (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"),
    )
)
