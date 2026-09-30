from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
        F.sum(F.when(F.col("p.method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
    )
    .orderBy("bank_transfer_count", "opened_year")
    .limit(50)
)
