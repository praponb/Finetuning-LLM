from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .withColumn("amount_rounded", F.round("amount", 2))
)
