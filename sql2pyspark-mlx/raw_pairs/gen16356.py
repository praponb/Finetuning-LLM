from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .filter(F.col("cost_range") >= 41)
    .orderBy("avg_cost")
)
