from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1620, "high").when(F.col("amount") >= 420, "mid").otherwise("low").alias("amount_bucket"),
        F.month("paid_at").alias("paid_month"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("num_payments"),
    )
    .filter(F.col("num_payments") > 3)
    .orderBy("paid_month", "amount_bucket")
)
