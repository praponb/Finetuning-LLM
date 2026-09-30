from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.customer_id"))
    .agg(
        F.sum(F.when(F.col("s.status") == "IN_TRANSIT", F.col("s.cost")).otherwise(0)).alias("in_transit_cost"),
    )
)
