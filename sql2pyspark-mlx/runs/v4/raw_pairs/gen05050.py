from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
    .orderBy(F.col("opened_year").desc())
)
