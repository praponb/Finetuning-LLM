from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 61000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("account_type") == "LOAN", 1)), F.lit(0)), 2).alias("balance_per_loan"),
        F.avg(F.when(F.col("account_type") == "LOAN", F.col("balance"))).alias("loan_avg_balance"),
    )
    .filter(F.col("loan_avg_balance") >= 0)
    .orderBy(F.col("loan_avg_balance").asc_nulls_first())
)
