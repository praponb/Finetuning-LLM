from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "launch_date",
        "stock_qty",
        "brand",
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
    )
)
