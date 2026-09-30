from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(560, 780))
    .select(
        "id",
        "tags",
        (F.col("stock_qty") + F.col("price")).alias("stock_qty_plus_price"),
    )
    .orderBy("stock_qty_plus_price")
)
