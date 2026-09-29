from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.order_id").isin([7, 25, 31, 35]) | (F.col("p.paid_at") < "2024-12-01"))
    .select(F.col("o.discount"), F.col("o.customer_id"), F.col("p.amount"))
    .orderBy(F.col("discount").desc(), F.col("amount").desc())
)
