from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-06-01")
    .withColumn("paid_at_year", F.year("paid_at"))
)
