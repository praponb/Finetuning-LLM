from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("order_id") == 16)
    .withColumn("shipped_date_minus_78d", F.date_sub("shipped_date", 78))
)

agg = (
    recent_shipments
    .groupBy("status")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.max("shipped_date_minus_78d").alias("last_shipped_date_minus_78d"),
    )
)

result = (
    agg
    .orderBy("status", F.col("cost_range").desc())
    .limit(5)
)
