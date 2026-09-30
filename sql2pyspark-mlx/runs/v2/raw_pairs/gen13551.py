from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .orderBy("opened_quarter")
)
