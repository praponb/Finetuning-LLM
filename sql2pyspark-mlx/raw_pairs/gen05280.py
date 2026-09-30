from pyspark.sql import functions as F

min_stock_qty = df_products.agg(F.min("stock_qty")).first()[0]

result = (
    df_products
    .filter(F.col("stock_qty") <= min_stock_qty)
    .select("id", "stock_qty")
    .orderBy(F.col("id").desc(), F.col("stock_qty").desc())
    .limit(20)
)
