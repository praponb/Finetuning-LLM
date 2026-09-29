from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 45)
    .groupBy(
        "order_id",
        "carrier",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.avg("cost").alias("avg_cost"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
    .orderBy("order_id")
)
