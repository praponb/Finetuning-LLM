from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 870)
    .select(
        "id",
        "category",
        F.length("category").alias("category_length"),
        F.col("stock_qty").cast("double").alias("stock_qty_num"),
    )
)
