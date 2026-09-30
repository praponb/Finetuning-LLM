from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("amount").between(660, 1280) & (F.col("status") == "SUCCESS"))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") < 25)
    .orderBy("avg_amount", F.col("paid_quarter").desc())
)
