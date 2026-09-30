from pyspark.sql import functions as F

recent_payments = df_payments.filter((F.col("method") == "paypal") & (F.col("amount") >= 1140))

method_summary = (
    recent_payments
    .groupBy("method")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = method_summary
