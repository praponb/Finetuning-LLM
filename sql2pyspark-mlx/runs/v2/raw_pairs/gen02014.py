from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(560, 860))
    .agg(F.avg("price").alias("avg_price"))
    .orderBy(F.col("avg_price").desc())
)
