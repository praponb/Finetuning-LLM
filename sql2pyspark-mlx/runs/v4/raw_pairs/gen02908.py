from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter(F.col("s.status").isin(["IN_TRANSIT", "LOST"]))
    .groupBy(F.col("o.store_id"))
    .agg(
        F.max(F.col("s.cost")).alias("max_cost"),
        F.countDistinct(F.col("s.cost")).alias("unique_carriers"),
    )
    .orderBy("store_id", F.col("unique_carriers").desc())
)
