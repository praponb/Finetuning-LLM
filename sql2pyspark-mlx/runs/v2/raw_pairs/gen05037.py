from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .groupBy("status")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
