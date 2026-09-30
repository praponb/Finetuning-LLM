from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(52000, 72000))
    .groupBy("status")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy(F.col("avg_balance").desc())
)
