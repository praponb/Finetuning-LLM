from pyspark.sql import functions as F

result = (
    df_products
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy(F.col("avg_price").desc())
)
