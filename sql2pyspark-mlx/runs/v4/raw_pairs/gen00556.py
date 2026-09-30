from pyspark.sql import functions as F

matched_keys = (
    df_payments
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([2, 21]))
        .select("status")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("YEAR", "paid_at").alias("year_start"), "method")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
    )
)
