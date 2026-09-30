from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(174, 357))
    .select(
        "id",
        "brand",
        "supplier_id",
        "launch_date",
        F.substring("product_name", 1, 3).alias("product_name_prefix"),
    )
)
