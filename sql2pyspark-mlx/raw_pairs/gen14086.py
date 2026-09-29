from pyspark.sql import functions as F

src = df_payments.filter(F.to_date("paid_at") >= "2024-05-01")

agg = (
    src
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    agg
    .filter(F.col("avg_amount") >= 20)
    .select("status", "avg_amount")
    .orderBy("status", F.col("avg_amount").desc())
)
