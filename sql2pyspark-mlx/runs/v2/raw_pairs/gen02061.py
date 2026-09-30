from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "status", F.trim("status").alias("clean_status"))
    .orderBy("status", "id")
    .limit(10)
)
