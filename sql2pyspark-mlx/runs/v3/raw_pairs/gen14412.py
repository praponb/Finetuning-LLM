from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(F.avg("discount").alias("avg_discount"))
)
