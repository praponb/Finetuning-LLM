from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([10, 23]))
    .select(
        "status",
        "balance",
        "opened_date",
        F.when(F.col("account_type") == "CHECKING", 1).otherwise(0).alias("is_checking"),
        F.when(F.col("account_type") == "LOAN", 1).otherwise(0).alias("is_loan"),
    )
)
