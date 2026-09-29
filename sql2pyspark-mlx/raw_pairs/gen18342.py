from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Acme"]) & F.array_contains("tags", "eco"))
    .select("id", "price")
    .orderBy("id", F.col("price").desc())
)
