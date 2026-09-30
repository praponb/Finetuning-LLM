from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("avg_balance") >= 43000)
    .orderBy(F.col("opened_quarter").desc())
)
