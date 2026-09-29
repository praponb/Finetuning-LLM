from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count("balance").alias("balance_count"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
    .filter(F.col("closed_count") >= 3)
    .orderBy(F.col("stddev_balance").desc(), "closed_count")
)
