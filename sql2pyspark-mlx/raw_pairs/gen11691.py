from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & F.col("customer_id").isin([1, 23, 26]) & (F.col("opened_date") <= "2024-12-13"))
    .select(
        "account_type",
        "opened_date",
        "customer_id",
        F.concat("status", F.lit(" - "), "account_type").alias("status_account_type"),
        F.floor("balance").alias("balance_whole"),
    )
    .orderBy("balance_whole")
)
