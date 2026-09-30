from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("amount") >= 1070) & (F.col("status").isin(["REFUNDED", "SUCCESS"]) | (F.col("paid_at") >= "2023-07-01")))
)

paid_month_summary = (
    filtered_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = paid_month_summary.select("paid_month", "method_values")
