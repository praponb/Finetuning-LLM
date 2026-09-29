from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(131, 399) & (F.array_contains("tags", "bestseller") | F.array_contains("tags", "premium")))
    .select("price", F.coalesce("price", F.lit(0)).alias("price_filled"))
)
