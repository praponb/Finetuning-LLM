from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1200) & ((F.col("paid_at") < "2023-07-01") | F.col("order_id").isin([14, 29, 30])))
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .orderBy(F.col("avg_amount").desc(), "status")
)
