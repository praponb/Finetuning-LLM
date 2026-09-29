from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"),
        F.col("o.channel"),
    )
    .agg((F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"))
    .orderBy(F.col("order_month").desc(), "channel")
    .limit(20)
)
