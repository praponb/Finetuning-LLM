from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(F.max("paid_at").alias("last_paid_at"))
    .orderBy(F.col("method").desc())
)
