from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["paypal", "card"]) & F.col("order_id").isNotNull() & (F.col("amount") <= 550))
    .groupBy(
        F.when(F.col("amount") >= 1290, "high").when(F.col("amount") >= 770, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
    )
)
