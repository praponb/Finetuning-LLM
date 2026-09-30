from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount", "status")
    .orderBy("status", F.col("amount").desc())
)
