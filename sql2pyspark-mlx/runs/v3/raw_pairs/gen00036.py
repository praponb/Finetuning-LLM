from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "Kerry", "UPS"]) | F.col("shipped_date").between("2023-04-06", "2023-12-01"))
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
)

shipments_stats = (
    src
    .groupBy("order_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = shipments_stats.select("order_id", "avg_cost")
