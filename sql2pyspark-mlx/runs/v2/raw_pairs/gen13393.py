from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "promptpay", "cash"]) | (F.col("method") == "bank_transfer"))
)

status_summary = (
    filtered_payments
    .groupBy("status")
    .agg(F.count(F.when(F.col("method") == "card", 1)).alias("card_count"))
)

result = (
    status_summary
    .orderBy("status")
    .limit(10)
)
