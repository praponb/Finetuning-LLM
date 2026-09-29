from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.max("balance").alias("max_balance"),
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("max_balance") > 31000)
    .orderBy(F.col("max_balance").desc())
)
