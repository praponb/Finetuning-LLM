from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .orderBy(F.col("opened_month").desc(), "status")
    .limit(20)
)
