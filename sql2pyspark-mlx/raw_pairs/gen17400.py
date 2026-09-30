from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 780)
    .select("id", "amount", "method", F.hour("paid_at").alias("paid_at_hour"))
    .orderBy("method", "id")
    .limit(20)
)
