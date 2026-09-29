from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.status").isin(["SUCCESS", "REFUNDED"]) & F.col("p.status").isin(["REFUNDED", "SUCCESS"]))
    .select(
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.status").alias("order_status"),
    )
    .orderBy(F.col("status").desc())
)
