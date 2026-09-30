from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"), "status")
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
)
