from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 28)
    .groupBy("account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)
