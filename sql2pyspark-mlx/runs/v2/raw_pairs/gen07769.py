from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2025-08-01")
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") < 8)
    .select("paid_month", "status_values", "amount_count")
    .orderBy(F.col("amount_count").desc(), F.col("paid_month").desc())
    .limit(10)
)
