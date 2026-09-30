from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .groupBy("status", "method")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("amount_range") < 76)
)
