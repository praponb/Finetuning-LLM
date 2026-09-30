from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2023-09-01") & F.col("amount").between(1150, 1530))
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .filter(F.col("avg_amount") > 680)
    .orderBy("avg_amount", "paid_year")
    .limit(20)
)
