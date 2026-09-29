from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "stock_qty",
        F.round((F.col("stock_qty") / F.col("cost")), 2).alias("stock_qty_per_cost"),
    )
)
