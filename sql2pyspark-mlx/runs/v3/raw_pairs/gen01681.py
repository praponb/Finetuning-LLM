from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("status").isNull() & (F.col("balance") <= 28000))
    .select(
        "account_type",
        "status",
        "balance",
        F.concat("status", F.lit(", "), "account_type").alias("status_account_type"),
        F.date_trunc("month", "opened_date").alias("opened_date_period"),
    )
    .orderBy("status", "opened_date_period")
)
