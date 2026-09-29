from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 840)
    .select(
        "id",
        "price",
        "stock_qty",
        "cost",
        (F.col("price") + F.col("stock_qty")).alias("price_plus_stock_qty"),
    )
)
