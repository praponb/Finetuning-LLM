from pyspark.sql import functions as F

max_price = df_products.filter(F.col("stock_qty") >= 341).agg(F.max("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") >= max_price)
    .select("id", "price", "product_name", "brand")
    .orderBy(F.col("product_name").desc(), "id")
    .limit(10)
)
