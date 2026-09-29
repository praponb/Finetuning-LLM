from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.method").isNull())
    .select(
        F.col("o.order_date"),
        F.col("p.paid_at"),
        F.col("p.amount"),
        F.col("p.order_id"),
        F.when(F.col("p.amount") >= 1360, "High").when(F.col("p.amount") >= 340, "Medium").otherwise("Low").alias("amount_band"),
    )
)
