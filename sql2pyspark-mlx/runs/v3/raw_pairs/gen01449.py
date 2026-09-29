from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("store_id").isNull() & (F.col("store_id") == 19))
    .withColumn("discount_rounded", F.round("discount", 2))
)

status_summary = (
    filtered_orders
    .groupBy("status")
    .agg(F.sum("discount_rounded").alias("total_discount_rounded"))
)

result = (
    status_summary
    .filter(F.col("total_discount_rounded") < 180)
    .select("status", "total_discount_rounded")
    .orderBy(F.col("status").desc())
)
