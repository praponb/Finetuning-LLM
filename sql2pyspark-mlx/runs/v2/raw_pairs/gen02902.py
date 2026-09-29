from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Fashion"]) & F.col("stock_qty").between(181, 289))
    .groupBy("category")
    .agg(F.count("*").alias("num_products"))
    .orderBy("category")
)
