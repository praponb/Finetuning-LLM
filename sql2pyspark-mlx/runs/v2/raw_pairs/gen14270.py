from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 57000) & F.col("customer_id").isNotNull())
    .select(
        "account_type",
        "customer_id",
        "balance",
        F.floor("balance").alias("balance_whole"),
    )
    .orderBy("customer_id", "balance_whole")
)
