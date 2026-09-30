from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("total_amount") >= 13000)
    .orderBy(F.col("total_amount").desc())
)
