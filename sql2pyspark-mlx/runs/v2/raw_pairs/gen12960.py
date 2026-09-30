from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isin(["East", "West", "Central"]))
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.status").alias("order_status"),
        F.col("o.channel"),
        F.col("s.sqft"),
    )
    .orderBy("channel")
)
