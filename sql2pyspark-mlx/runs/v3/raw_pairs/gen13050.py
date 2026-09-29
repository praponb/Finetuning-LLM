from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .orderBy("last_opened_date", F.col("loan_balance").desc(), "opened_year")
    .limit(5)
)
