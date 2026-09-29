from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(265, 311) & F.col("stock_qty").isNotNull())
    .select("category", F.substring("category", 1, 4).alias("category_prefix"))
)
