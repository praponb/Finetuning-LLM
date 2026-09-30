from pyspark.sql import functions as F

avg_price = df_products.agg(F.avg("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") >= avg_price)
    .select("id", "price")
    .orderBy("id", F.col("price").desc())
)
