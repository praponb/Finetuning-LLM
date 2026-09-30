from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("avg_balance") < 21)
    .orderBy("avg_balance", "status")
)
