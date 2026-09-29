from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.max("balance").alias("max_balance"),
    )
    .orderBy("opened_month")
)
