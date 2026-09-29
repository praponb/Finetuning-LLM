from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() | (F.col("account_type") == "LOAN"))
    .select(
        "id",
        "customer_id",
        "balance",
        "account_type",
        F.concat("status", F.lit(" - "), "account_type").alias("status_account_type"),
    )
    .orderBy(F.col("status_account_type").desc(), "id")
    .limit(20)
)
