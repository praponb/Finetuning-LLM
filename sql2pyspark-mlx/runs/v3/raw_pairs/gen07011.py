from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]) & F.col("method").isin(["paypal", "cash", "bank_transfer"])))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
    .orderBy("stddev_amount")
)
