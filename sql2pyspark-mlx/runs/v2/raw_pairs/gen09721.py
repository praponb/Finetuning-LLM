from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.min("balance").alias("min_balance"),
    )
    .orderBy(F.col("opened_month").desc(), F.col("min_balance").desc())
)
