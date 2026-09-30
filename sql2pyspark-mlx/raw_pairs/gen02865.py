from pyspark.sql import functions as F

max_price = df_products.filter(F.array_contains("tags", "clearance")).agg(F.max("price")).first()[0]

result = (
    df_products
    .filter(F.col("price") >= max_price)
    .select("id", "price")
    .orderBy(F.col("price").desc())
)
