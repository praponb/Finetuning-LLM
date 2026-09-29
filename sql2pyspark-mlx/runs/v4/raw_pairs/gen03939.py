from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", "status", F.month("shipped_date").alias("shipped_month"))
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") < 42)
    .orderBy(F.col("shipped_month").desc(), "order_id", "status")
    .limit(10)
)
