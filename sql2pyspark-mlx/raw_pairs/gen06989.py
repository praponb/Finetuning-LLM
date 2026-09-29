from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "LOAN"]) & F.col("customer_id").isNull())
    .select(
        "id",
        "status",
        "balance",
        F.round("balance", 1).alias("balance_rounded"),
        F.upper("status").alias("status_upper"),
    )
)
