from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select(
        "product_name",
        F.round((F.col("stock_qty") / F.col("cost")), 2).alias("stock_qty_per_cost"),
    )
    .orderBy("product_name", F.col("stock_qty_per_cost").desc())
)
