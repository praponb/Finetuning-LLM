from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("order_id") == 26)

summary = (
    valid_shipments
    .groupBy("status", "carrier")
    .agg(
        F.min("shipped_date").alias("first_shipped_date"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = summary.filter(F.col("avg_cost") >= 94)
