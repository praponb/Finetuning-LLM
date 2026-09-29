from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("status") == "REFUNDED") & (F.col("status") == "FAILED")))
    .withColumn("paid_at_day", F.col("paid_at").cast("date"))
    .withColumn("amount_band", F.when(F.col("amount") >= 1490, "High").when(F.col("amount") >= 630, "Medium").otherwise("Low"))
    .orderBy(F.col("order_id").desc(), "method")
)
