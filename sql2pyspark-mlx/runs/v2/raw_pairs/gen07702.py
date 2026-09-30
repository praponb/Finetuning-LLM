from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("cost_range") < 36)
    .select("order_id", "cost_range", "avg_cost")
    .orderBy("cost_range", "order_id")
    .limit(3)
)
