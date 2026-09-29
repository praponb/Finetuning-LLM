from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"), "merchant")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
    .orderBy("merchant")
)
