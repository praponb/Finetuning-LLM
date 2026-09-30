from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "promptpay", "cash"]))
    .groupBy("status")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .filter(F.col("stddev_amount") < 32)
    .orderBy("status")
)
