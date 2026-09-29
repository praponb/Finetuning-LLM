from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum("amount").alias("total_amount"),
    )
)
