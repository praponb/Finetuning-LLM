from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(54000, 60000) | (F.col("balance") >= 44000))
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy(F.col("opened_quarter").desc())
)
