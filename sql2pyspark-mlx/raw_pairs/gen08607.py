from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 26000) & (F.col("balance") > 70000) & (F.col("account_type") == "CHECKING"))
)
