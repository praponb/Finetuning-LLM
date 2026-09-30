from pyspark.sql import functions as F

max_price = df_products.filter(F.col("product_name").like("%Backp%")).agg(F.max("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") >= max_price)
    .select("id", "price")
    .orderBy(F.col("id").desc())
    .limit(50)
)
