from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.discount").between(0.16, 0.28))
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(
        F.countDistinct(F.col("s.cost")).alias("unique_statuss"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .orderBy("channel")
)
