from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.hour("paid_at").between(10, 17)).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .filter(F.col("o.status").isin(["PENDING", "COMPLETED", "CANCELLED"]))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
    )
    .orderBy("id")
)
