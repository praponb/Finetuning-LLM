from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("paid_at").alias("first_paid_at"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 810)
    .drop("min_amount")
    .orderBy("status")
)
