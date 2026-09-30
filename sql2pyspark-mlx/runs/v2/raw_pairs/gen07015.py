from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
    .filter(F.col("avg_discount") > 29)
    .orderBy("status")
    .limit(1)
)
