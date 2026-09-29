from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1000)
    .select("method", "status")
    .distinct()
)
