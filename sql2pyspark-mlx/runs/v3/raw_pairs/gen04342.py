from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "card")
    .select("id", "method", "status", "amount", F.upper("method").alias("method_upper"))
)
