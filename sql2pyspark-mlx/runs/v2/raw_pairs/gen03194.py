from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "supplier_id",
        F.least("price", "cost").alias("least_price_cost"),
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
    )
    .orderBy(F.col("stock_qty_filled").desc())
)
