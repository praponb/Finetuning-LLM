from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["SHIPPED", "PENDING"]))
    .rollup("channel", "status")
    .agg(F.avg("discount").alias("avg_discount"))
    .orderBy("channel")
)
