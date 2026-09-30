from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(~((F.col("txn_type") == "FEE") & (F.to_date("txn_time") < "2025-03-29")))
)

agg = (
    recent_transactions
    .groupBy("merchant")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = agg
