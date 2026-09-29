from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("amount") < 530)
    .groupBy("order_id")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.max("paid_at").alias("latest_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.unique_orders") > 6)
    .select(
        F.col("o.order_date"),
        F.col("ot.unique_orders"),
        F.col("ot.latest_paid_at"),
    )
)
