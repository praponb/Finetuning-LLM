from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("p.paid_at"),
        F.col("p.order_id"),
        F.col("o.order_date"),
        F.coalesce(F.col("p.amount"), F.lit(0)).alias("amount_filled"),
    )
    .orderBy(F.col("order_date").desc())
)
