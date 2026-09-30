from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .filter(F.col("s.order_id").isNotNull())
    .join(df_orders.filter(F.col("status").isin(["CANCELLED", "PENDING"])).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.order_id") == 6)
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.order_id"),
        F.col("s.carrier"),
    )
)
