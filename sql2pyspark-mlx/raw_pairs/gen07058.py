from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.hour("paid_at").between(8, 15))
    .withColumn("paid_at_period", F.date_trunc("WEEK", "paid_at"))
)

agg = (
    base
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1600, "high").when(F.col("amount") >= 690, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    agg
    .filter(F.col("max_amount") > 1110)
    .select("status", "amount_bucket", "max_amount")
)
