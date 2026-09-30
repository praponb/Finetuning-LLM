from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "cash") & (F.col("method") == "cash"))
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy("min_amount")
)
