from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count("*").alias("cnt"),
    )
)
