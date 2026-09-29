from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 60000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        "customer_id",
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
)
