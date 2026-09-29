from pyspark.sql import functions as F

src = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & ((F.col("status") != "SUCCESS") | F.col("method").isin(["paypal", "card", "promptpay"])))
    .withColumn("days_since_paid_at", F.datediff(F.current_date(), "paid_at"))
)

status_summary = (
    src
    .groupBy("status")
    .agg(F.max("paid_at").alias("latest_paid_at"))
)

result = status_summary.select("status", "latest_paid_at")
