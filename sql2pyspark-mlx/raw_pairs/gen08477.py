from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1340)
    .select("amount", "method")
    .orderBy(F.col("method").desc())
)
