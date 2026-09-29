from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(6, 12))
    .groupBy("status")
    .agg(F.count("*").alias("payment_count"))
    .filter(F.col("payment_count") > 3)
    .orderBy("payment_count", "status")
    .limit(3)
)
