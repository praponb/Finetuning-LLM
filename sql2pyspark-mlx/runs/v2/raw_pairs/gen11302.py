from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(26000, 41000) | F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]))
    .select(
        "id",
        "balance",
        "account_type",
        F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0).alias("is_savings"),
    )
)
