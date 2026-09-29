from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "stock_qty",
        "supplier_id",
        "tags",
        (F.col("price") + F.col("cost")).alias("margin"),
        F.floor("cost").alias("cost_whole"),
    )
)
