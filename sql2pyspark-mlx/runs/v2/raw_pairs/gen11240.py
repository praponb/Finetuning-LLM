from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(F.sum("discount").alias("total_discount"))
    .orderBy(F.col("status").desc(), "channel")
    .limit(20)
)
