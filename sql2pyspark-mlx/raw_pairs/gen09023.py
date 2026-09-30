from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2024-11-01")
    .withColumn("paid_at_date", F.to_date("paid_at"))
    .withColumn("paid_at_month", F.month("paid_at"))
)
