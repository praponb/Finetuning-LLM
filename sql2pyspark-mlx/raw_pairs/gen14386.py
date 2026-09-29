from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "stock_qty",
        "product_name",
        "tags",
        (F.col("cost") - F.col("stock_qty")).alias("cost_minus_stock_qty"),
    )
    .orderBy(F.col("id").desc(), "product_name")
)
