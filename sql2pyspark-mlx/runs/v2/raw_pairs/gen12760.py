from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNotNull())
    .select(
        "id",
        "tags",
        "category",
        (F.col("price") + F.col("cost")).alias("margin"),
        F.floor("stock_qty").alias("stock_qty_whole"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
