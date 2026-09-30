from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "account_type")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.max("balance").alias("max_balance"),
    )
    .orderBy(F.col("stddev_balance").desc())
)
