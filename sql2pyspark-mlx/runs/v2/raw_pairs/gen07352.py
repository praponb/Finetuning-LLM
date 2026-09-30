from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.cost") < 33)
    .groupBy(F.col("o.status"))
    .agg(
        F.max(F.col("s.cost")).alias("max_cost"),
        F.countDistinct(F.col("s.cost")).alias("unique_orders"),
    )
    .orderBy("max_cost", "status")
    .limit(5)
)
