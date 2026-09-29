from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNull() | F.col("product_name").like("%Yoga%"))
    .select(
        "id",
        "cost",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
    )
)
