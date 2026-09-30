from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(6, 12) & (F.to_date("paid_at") < "2024-04-12"))
    .groupBy(
        F.when(F.col("amount") >= 1260, "high").when(F.col("amount") >= 660, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.min("amount").alias("min_amount"))
    .orderBy("min_amount")
)
