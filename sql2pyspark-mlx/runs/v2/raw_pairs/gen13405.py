from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") < 69000) & (F.col("opened_date") <= "2024-06-01"))
    .select(
        "account_type",
        "balance",
        F.when(F.col("status") == "OPEN", 1).otherwise(0).alias("is_open"),
    )
)
