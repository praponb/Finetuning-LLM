from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("paid_at") < "2024-06-28")

paid_month_summary = (
    filtered_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    paid_month_summary
    .select("paid_month", "status_values")
    .orderBy("paid_month")
)
