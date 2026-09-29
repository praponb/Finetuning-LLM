from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "launch_date",
        "category",
        "stock_qty",
        (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"),
    )
)
