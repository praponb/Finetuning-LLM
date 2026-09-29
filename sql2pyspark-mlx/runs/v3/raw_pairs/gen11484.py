from pyspark.sql import functions as F

base = df_payments.filter(F.col("amount").between(1390, 1480))

agg = (
    base
    .groupBy("method")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    agg
    .filter(F.col("avg_amount") >= 900)
    .orderBy(F.col("avg_amount").desc())
)
