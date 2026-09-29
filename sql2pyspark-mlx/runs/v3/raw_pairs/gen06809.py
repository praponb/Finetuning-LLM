from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("amount").between(1330, 1590) & ((F.col("amount") > 1420) | (F.col("status") == "REFUNDED")))
)

summary = (
    filtered_payments
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1280, "high").when(F.col("amount") >= 660, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
    )
)

result = summary.filter(F.col("success_amount") < 47)
