from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.sum("balance").alias("total_balance"),
    )
)
