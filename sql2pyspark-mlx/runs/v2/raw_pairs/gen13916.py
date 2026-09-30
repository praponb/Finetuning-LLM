from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.max("opened_date").alias("last_opened_date"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .filter(F.col("stddev_balance") < 70)
)
