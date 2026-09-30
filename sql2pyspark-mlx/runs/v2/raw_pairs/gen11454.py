from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") <= "2023-11-01") & (F.col("status") == "CLOSED"))
    .groupBy("status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("avg_balance") > 58)
    .orderBy(F.col("avg_balance").desc())
)
