from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.year("delivered_date") >= 2025))
    .select("*", F.last_day("delivered_date").alias("delivered_date_month_end"))
)

result = (
    cleaned
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "left")
    .groupBy(
        F.col("dim.channel").alias("channel"),
        F.col("dim.status").alias("order_status"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.cost")).alias("min_cost"),
    )
    .orderBy("shipment_status")
)
