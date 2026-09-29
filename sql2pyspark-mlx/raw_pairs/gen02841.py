from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(113, 131))
    .select(
        "launch_date",
        "stock_qty",
        "brand",
        F.array_join("tags", "|").alias("tags_text"),
        F.col("price").cast("int").alias("price_int"),
    )
)
