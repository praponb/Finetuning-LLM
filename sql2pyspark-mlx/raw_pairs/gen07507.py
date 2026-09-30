from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.col("s.cost")).alias("total_cost"),
        F.min(F.col("s.cost")).alias("min_cost"),
    )
    .orderBy("total_cost", "status")
    .limit(5)
)
