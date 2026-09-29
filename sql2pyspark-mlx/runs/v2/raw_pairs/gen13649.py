from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "account_type",
        F.when(F.col("account_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
)
