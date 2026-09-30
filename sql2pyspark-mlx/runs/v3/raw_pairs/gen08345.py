from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([11, 17, 34, 38]))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)
