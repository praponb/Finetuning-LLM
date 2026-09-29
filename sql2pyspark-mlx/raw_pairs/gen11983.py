from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CREDIT"]) & (F.year("opened_date") == 2025))
    .groupBy(
        F.when(F.col("status").isin(["CLOSED", "OPEN"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") > 156000)
    .drop("total_balance")
)
