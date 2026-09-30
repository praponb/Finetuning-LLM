from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.status") == "REFUNDED")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.countDistinct(F.col("p.amount")).alias("unique_statuss"),
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
    )
    .orderBy("first_paid_at", "status")
    .limit(5)
)
