from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category") != "Fashion")
    .select(
        "id",
        "tags",
        "product_name",
        "cost",
        F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"),
    )
)
