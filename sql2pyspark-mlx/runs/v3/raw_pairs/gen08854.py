from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 11)
    .select(
        "stock_qty",
        "category",
        "brand",
        F.month("launch_date").alias("launch_date_month"),
    )
)
