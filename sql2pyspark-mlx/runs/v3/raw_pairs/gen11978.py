from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "LOAN"]) & ~F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select(
        "id",
        "customer_id",
        "status",
        "opened_date",
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
    )
)
