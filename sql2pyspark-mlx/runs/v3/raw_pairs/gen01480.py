from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED"]) | F.col("customer_id").isin([30, 34, 36, 40]))
    .select(
        "id",
        "opened_date",
        "balance",
        F.when(F.col("account_type") == "LOAN", 1).otherwise(0).alias("is_loan"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
