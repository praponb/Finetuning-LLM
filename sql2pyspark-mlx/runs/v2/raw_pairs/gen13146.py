from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        (F.col("price") * F.col("stock_qty")).alias("price_times_stock_qty"),
    )
)
