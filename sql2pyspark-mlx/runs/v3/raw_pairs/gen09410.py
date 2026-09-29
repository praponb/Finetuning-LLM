from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("amount") >= 1570)

payments_stats = (
    valid_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.count("*").alias("total_count"),
    )
)

result = (
    payments_stats
    .filter(F.col("total_count") >= 4)
    .select("paid_month", "method_values", "total_count")
)
