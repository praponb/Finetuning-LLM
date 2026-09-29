from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "OPEN") & F.col("customer_id").isNull() & (F.col("balance") < 27000))
    .select(
        "customer_id",
        "opened_date",
        "balance",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.concat("status", F.lit(" "), "account_type").alias("status_account_type"),
    )
)
