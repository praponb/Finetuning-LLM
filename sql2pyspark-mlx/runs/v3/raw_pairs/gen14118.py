from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2025-10-01")
    .groupBy("order_id", "status")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("stddev_cost") >= 41)
)
