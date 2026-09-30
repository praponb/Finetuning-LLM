from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("paid_at") >= "2024-10-20") & (F.col("method").isin(["cash", "promptpay"]) | (F.col("status") == "FAILED")))
)

status_summary = (
    filtered_payments
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = status_summary.select("status", "status_list")
