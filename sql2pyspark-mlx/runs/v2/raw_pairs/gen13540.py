from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("refunded_count") >= 6)
    .orderBy("method")
)
