from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-03-11").cast("date"))
    .withColumn("account_type_prefix", F.substring("account_type", 1, 2))
)

result = (
    valid
    .groupBy(
        "account_type",
        F.when(F.col("balance") >= 75000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
    )
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance_2"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_total_balance"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
)
