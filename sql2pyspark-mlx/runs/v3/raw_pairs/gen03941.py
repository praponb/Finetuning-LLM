from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "cash", "paypal"]))
    .groupBy(F.when(F.col("amount") >= 1340, "high").when(F.col("amount") >= 470, "mid").otherwise("low").alias("amount_bucket"))
    .pivot("status", ["SUCCESS", "REFUNDED"])
    .agg(F.sum("amount"))
    .orderBy("amount_bucket")
)
