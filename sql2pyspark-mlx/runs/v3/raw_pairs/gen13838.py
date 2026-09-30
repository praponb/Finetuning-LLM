from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.status").isin(["PENDING", "RETURNED"]))
    .select(
        F.col("orders.discount"),
        F.col("payments.amount"),
        F.col("payments.paid_at"),
        F.when(F.col("payments.amount") >= 1800, "High").when(F.col("payments.amount") >= 710, "Medium").otherwise("Low").alias("amount_level"),
    )
)
