from pyspark.sql import functions as F

valid_products = (
    df_products
    .filter((F.col("price") > 790) & ((F.col("stock_qty") < 100) | F.col("product_name").like("%Running%")))
)

products_stats = (
    valid_products
    .groupBy("brand")
    .agg(F.min("cost").alias("min_cost"))
)

result = products_stats.select("brand", "min_cost")
