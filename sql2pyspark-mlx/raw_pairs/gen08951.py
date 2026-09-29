from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "cash", "card"]))
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .pivot("method", ["bank_transfer", "card", "paypal", "cash"])
    .agg(F.avg("amount"))
)
