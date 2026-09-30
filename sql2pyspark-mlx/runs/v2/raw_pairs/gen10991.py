from pyspark.sql import functions as F

avg_price = df_products.filter(F.col("stock_qty") >= 219).agg(F.avg("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") > avg_price)
    .select("id", "price")
    .orderBy(F.col("id").desc())
)
