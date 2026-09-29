from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
        "account_type",
    )
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
    )
    .filter(F.col("unique_statuss") >= 6)
)
