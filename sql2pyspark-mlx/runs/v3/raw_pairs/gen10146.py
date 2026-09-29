from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").like("Old%") & F.col("p.status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM-dd").alias("opened_date_label"),
    )
)
