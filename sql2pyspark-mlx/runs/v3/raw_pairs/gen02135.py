from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["RETURNED", "CANCELLED", "COMPLETED"]))
    .groupBy("channel")
    .agg(F.avg("amount").alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc())
)
