from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 870) & (F.col("status") == "FAILED"))
    .withColumn("paid_at_label", F.date_format("paid_at", "MMM yyyy"))
    .orderBy(F.col("amount").desc(), "id")
    .limit(50)
)
