from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("pct_loan") > 28)
    .orderBy(F.col("customer_id").desc())
)
