from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(151, 152) | (F.size("tags") >= 2))
    .select("id", "brand", "price", F.floor("cost").alias("cost_whole"))
    .orderBy("price")
)
