from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.quarter("opened_date").alias("opened_quarter"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
    .orderBy(F.col("checking_balance").desc(), "opened_quarter", "opened_month")
    .limit(5)
)
