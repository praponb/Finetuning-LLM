from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.hour("paid_at").between(6, 11) & F.col("order_id").isNotNull() & (F.col("order_id") == 35))
)

payments_stats = (
    base
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    payments_stats
    .select("paid_month", "total_amount")
    .orderBy(F.col("total_amount").desc())
)
