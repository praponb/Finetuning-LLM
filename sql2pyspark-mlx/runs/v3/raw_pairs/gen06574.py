from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.max(F.col("p.paid_at")).alias("last_paid_at"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .orderBy(F.col("opened_quarter").desc(), F.col("max_amount").desc())
    .limit(3)
)
