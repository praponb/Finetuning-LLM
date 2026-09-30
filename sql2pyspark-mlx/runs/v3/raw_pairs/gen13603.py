from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "account_type",
    )
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .orderBy("avg_balance")
)
