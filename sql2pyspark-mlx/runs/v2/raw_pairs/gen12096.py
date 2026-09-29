from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "FedEx")
    .groupBy("status", "order_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("avg_cost") < 65)
    .select("status", "order_id", "avg_cost", "cost_range")
    .orderBy(F.col("status").desc(), "order_id")
    .limit(10)
)
