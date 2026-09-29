from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.status").isin(["CANCELLED", "RETURNED"]))
    .select(
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("p.status"),
        F.col("p.paid_at"),
        F.when(F.col("p.amount") >= 1290, "High").when(F.col("p.amount") >= 430, "Medium").otherwise("Low").alias("amount_band"),
    )
)
