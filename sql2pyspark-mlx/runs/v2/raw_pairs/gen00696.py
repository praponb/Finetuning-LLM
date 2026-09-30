from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("discount") >= 0.36).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.carrier") == "UPS")
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("s.status"),
    )
)
