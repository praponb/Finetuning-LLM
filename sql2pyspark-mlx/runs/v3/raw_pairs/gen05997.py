from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.count("*").alias("num_payments"),
    )
    .filter(F.col("num_payments") > 4)
    .orderBy(F.col("num_payments").desc(), "status", "method")
    .limit(10)
)
