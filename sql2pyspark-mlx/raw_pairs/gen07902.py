from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([4, 7, 12]))
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum("balance").alias("total_balance"),
    )
)
