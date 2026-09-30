from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .orderBy("paid_at", "id")
    .limit(20)
)
