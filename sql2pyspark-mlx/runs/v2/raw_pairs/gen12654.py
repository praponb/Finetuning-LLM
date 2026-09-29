from pyspark.sql import functions as F

result = (
    df_payments
    .select("method")
    .distinct()
    .orderBy(F.col("method").desc())
    .limit(5)
)
