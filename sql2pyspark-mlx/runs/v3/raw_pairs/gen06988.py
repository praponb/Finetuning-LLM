from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower("product_name").like("%ire%"))
    .select(
        "id",
        "stock_qty",
        "category",
        "supplier_id",
        F.col("stock_qty").cast("double").alias("stock_qty_str"),
    )
)
