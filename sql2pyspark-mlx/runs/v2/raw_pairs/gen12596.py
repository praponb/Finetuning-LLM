from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isNotNull() & (F.col("o.order_date") > "2025-10-01"))
    .select(
        F.col("o.discount"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("p.amount"),
    )
    .orderBy(F.col("amount").desc())
)
