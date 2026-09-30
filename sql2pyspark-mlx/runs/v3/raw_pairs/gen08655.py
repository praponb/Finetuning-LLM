from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "account_type",
    )
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") < 20)
)
