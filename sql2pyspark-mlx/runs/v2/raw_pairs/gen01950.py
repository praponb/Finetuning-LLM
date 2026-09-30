from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(680, 900) & F.col("brand").isin(["Globex", "Initech"]))
    .select(
        "id",
        "stock_qty",
        F.least("price", "stock_qty").alias("least_price_stock_qty"),
    )
    .orderBy("stock_qty")
)
