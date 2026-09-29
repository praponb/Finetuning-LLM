from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"), "account_type")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.countDistinct("balance").alias("unique_customers"),
    )
    .filter(F.col("closed_balance") < 69)
    .orderBy(F.col("unique_customers").desc(), "account_type", "opened_year")
    .limit(50)
)
