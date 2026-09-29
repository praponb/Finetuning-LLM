from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(128, 285))
    .groupBy("brand")
    .pivot("category", ["Home", "Sports"])
    .agg(F.count("cost"))
)
