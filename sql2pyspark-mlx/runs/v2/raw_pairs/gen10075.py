from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") >= 268000)
    .orderBy(F.col("account_type").desc())
    .limit(5)
)
