from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
    )
    .orderBy("latest_paid_at")
)
