from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.count("amount").alias("amount_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
