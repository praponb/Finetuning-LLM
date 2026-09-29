from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.date_trunc("MONTH", "paid_at").alias("paid_month"),
        F.year("paid_at").alias("paid_year"),
    )
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") >= 630)
    .orderBy("paid_month")
)
