from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.year("paid_at").alias("paid_year"))
    .pivot("method", ["bank_transfer", "cash"])
    .agg(F.max("amount"))
    .orderBy("paid_year")
)
