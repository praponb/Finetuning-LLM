from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("s.cost")).alias("cost_count"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .orderBy(F.col("total_cost").desc())
)
