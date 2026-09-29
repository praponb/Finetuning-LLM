from pyspark.sql import functions as F

base = df_payments.filter(F.col("amount") < 570)

method_summary = (
    base
    .groupBy("method")
    .agg(F.max("paid_at").alias("latest_paid_at"))
)

result = method_summary.select("method", "latest_paid_at")
