from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id").isNull())
    .groupBy(
        F.col("o.channel"),
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
    )
    .agg(
        F.max(F.col("s.shipped_date")).alias("latest_shipped_date"),
        F.max(F.col("s.cost")).alias("max_cost"),
    )
    .orderBy(F.col("channel").desc(), "order_month")
)
