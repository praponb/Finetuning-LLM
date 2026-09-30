from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.year("opened_date") == 2025) & ~F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
)

summary = (
    filtered_accounts
    .groupBy("account_type")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance")).otherwise(0)).alias("loan_balance"),
    )
)

result = (
    summary
    .filter(F.col("loan_balance") >= 77)
    .orderBy(F.col("last_opened_date").desc())
)
