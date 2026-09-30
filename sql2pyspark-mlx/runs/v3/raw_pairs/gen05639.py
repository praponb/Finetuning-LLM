from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("order_id").isNotNull() & F.month("shipped_date").isin([1, 7, 9])) | (F.col("order_id") == 13))
)

agg = (
    valid_shipments
    .groupBy("status")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    agg
    .filter(F.col("cost_range") < 73)
    .select("status", "cost_range")
    .orderBy("status", F.col("cost_range").desc())
)
