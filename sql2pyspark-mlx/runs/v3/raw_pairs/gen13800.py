from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "status", "method", "amount")
    .orderBy(F.col("id").desc())
)
