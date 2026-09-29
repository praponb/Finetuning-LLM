from pyspark.sql import functions as F

result = (
    df_payments
    .cube(
        "status",
        F.when(F.col("amount") >= 1580, "high").when(F.col("amount") >= 690, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("status")
)
