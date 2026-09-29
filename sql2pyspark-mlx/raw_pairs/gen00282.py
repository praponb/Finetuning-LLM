from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 20)
    .agg(F.avg("discount").alias("avg_discount"))
    .orderBy(F.col("avg_discount").desc())
)
