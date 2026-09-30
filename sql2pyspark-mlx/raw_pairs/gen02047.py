from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.max("amount").alias("max_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("max_amount") >= 1540)
)
