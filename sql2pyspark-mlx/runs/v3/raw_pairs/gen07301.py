from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("s.carrier"),
        F.col("s.status"),
        F.when(F.col("s.carrier").isin(["Kerry", "DHL"]), "Priority").otherwise("Other").alias("carrier_group"),
    )
    .orderBy(F.col("amount").desc(), F.col("status").desc())
)
