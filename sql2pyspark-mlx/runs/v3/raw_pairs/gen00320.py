from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "CLOSED")
    .groupBy("account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy("account_type")
)
