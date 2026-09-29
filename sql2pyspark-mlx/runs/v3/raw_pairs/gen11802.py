from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.region").isin(["Central", "South", "East"]) & (F.col("s.region") == "Central"))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.status"),
        F.col("o.status").alias("order_status"),
        F.col("o.amount"),
        F.col("s.sqft"),
    )
    .orderBy("id")
)
