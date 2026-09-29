from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([1, 19, 30, 38]) & F.col("customer_id").isin([1, 2, 14]) & (F.col("balance") <= 39000))
    .select(
        "account_type",
        "customer_id",
        "opened_date",
        F.concat("status", F.lit(" - "), "account_type").alias("status_account_type"),
    )
)
