from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"), "status")
    .agg(
        F.count("balance").alias("balance_count"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("balance_count") >= 1)
    .orderBy("balance_count")
)
