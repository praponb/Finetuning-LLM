from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount")
    .intersect(df_products.select(F.col("price").alias("amount")))
    .orderBy(F.col("amount").desc())
)
