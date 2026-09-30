from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("s.carrier").isin(["FedEx", "Flash", "UPS"]))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.carrier"),
        F.col("o.store_id"),
        F.col("o.amount"),
    )
)
