from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(F.avg("balance").alias("avg_balance"))
    .orderBy("avg_balance", "account_type", "opened_month")
    .limit(5)
)
