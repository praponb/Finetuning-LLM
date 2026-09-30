from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(~F.col("s.carrier").isin(["Kerry", "DHL"]))
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"))
    .agg(
        F.count(F.col("s.cost")).alias("cost_count"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .filter(F.col("total_cost") > 250)
    .orderBy(F.col("total_cost").desc(), F.col("order_month").desc())
    .limit(5)
)
