from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["CHECKING", "LOAN", "SAVINGS"]) & (F.col("account_type") != "LOAN"))
    .select(
        "id",
        "status",
        "account_type",
        F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0).alias("is_savings"),
    )
)
