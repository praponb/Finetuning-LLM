from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS", "CREDIT"]) | (F.col("balance") > 65000))
    .select(
        "account_type",
        F.round((F.col("balance") * 100), 2).alias("balance_adjusted"),
    )
    .orderBy("balance_adjusted")
)
