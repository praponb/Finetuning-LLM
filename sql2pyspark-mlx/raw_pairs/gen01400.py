from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .withColumn("status_prefix", F.substring("status", 1, 3))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
)
