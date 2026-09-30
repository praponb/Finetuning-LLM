from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.countDistinct("balance").alias("unique_customers"),
    )
    .filter(F.col("unique_customers") > 9)
    .orderBy(F.col("unique_customers").desc())
)
