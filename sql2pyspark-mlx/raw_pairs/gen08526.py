from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") != 263)
    .select("product_name")
    .intersect(df_products.filter(F.col("price") <= 1190).select("product_name"))
)
