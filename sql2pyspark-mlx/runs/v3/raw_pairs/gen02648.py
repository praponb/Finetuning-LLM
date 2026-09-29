from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "REFUNDED") & (F.col("amount") <= 1010))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(F.countDistinct("amount").alias("unique_methods"))
)
