from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance"))
    .select(
        "product_name",
        "category",
        "stock_qty",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
    )
    .orderBy("stock_qty")
)
