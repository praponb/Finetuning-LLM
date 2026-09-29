from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-12-31") & F.col("method").isin(["card", "cash"]) & (F.col("amount") <= 560))
    .groupBy("method")
    .agg(F.max("paid_at").alias("last_paid_at"))
)
