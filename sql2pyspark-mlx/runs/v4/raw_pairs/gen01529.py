from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)
