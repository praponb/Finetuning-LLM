from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "mobile", "web"]))
    .groupBy("status")
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
    .filter(F.col("avg_discount") >= 28)
    .orderBy(F.col("status").desc())
)
