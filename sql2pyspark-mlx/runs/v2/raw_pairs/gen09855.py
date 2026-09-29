from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", F.year("order_date").alias("order_year"))
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
    .filter(F.col("avg_discount") >= 71)
    .orderBy(F.col("order_year").desc(), "channel")
)
