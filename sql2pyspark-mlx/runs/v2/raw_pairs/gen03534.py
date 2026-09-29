from pyspark.sql import functions as F

base = (
    df_orders
    .filter(((F.col("amount") > 710) & F.col("channel").isin(["mobile", "store"])) | F.col("store_id").isin([31, 37]))
)

summary = (
    base
    .groupBy("status", "store_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    summary
    .filter(F.col("avg_amount") >= 1090)
    .select("status", "store_id", "avg_amount")
    .orderBy("status", "store_id")
    .limit(5)
)
