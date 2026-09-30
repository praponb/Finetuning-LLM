from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("method").isin(["cash", "paypal", "card"]))

payments_stats = (
    filtered_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = payments_stats.select("paid_month", "latest_paid_at", "status_values")
