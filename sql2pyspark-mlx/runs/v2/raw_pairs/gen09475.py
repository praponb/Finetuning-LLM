from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 35)
    .groupBy("channel")
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
    .filter(F.col("avg_discount") > 50)
    .select("channel", "avg_discount")
    .orderBy(F.col("channel").desc())
    .limit(5)
)
