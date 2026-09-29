from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & (F.col("account_type") == "LOAN"))
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        "account_type",
        "customer_id",
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.max("balance").alias("max_balance"),
    )
    .orderBy("savings_balance")
)
