from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.min("balance").alias("min_balance"),
    )
    .orderBy("pct_credit", F.col("opened_quarter").desc())
)
